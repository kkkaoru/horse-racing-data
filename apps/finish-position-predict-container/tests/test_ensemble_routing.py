"""Tests for per-race ensemble routing (Phase B-2E).

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
"""

from __future__ import annotations

import dataclasses
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib import per_class
from predict_lib.booster_pool import BoosterPool
from predict_lib.ensemble_routing import (
    EnsembleRouteOutcome,
    init_member_pool,
    score_race_with_resolution,
)
from predict_lib.per_class import EnsembleMember, PerClassEnsemble
from predict_lib.scorer import BoosterLike

JRA_FALLBACK_MODEL_VERSION: str = "iter14-jra-cb-pacestyle-course-v8"
# Mirrors the registry entry in ``predict_lib.per_class.PER_CLASS_MODEL_VERSIONS``
# — 703 was flipped from iter 23 to iter 26 v4 on 2026-06-05 (+0.189pp top1).
JRA_CLASS_703_ENSEMBLE_MODEL_VERSION: str = "iter26-jra-cb-ensemble-703-v8"
ITER22_RESIDUAL_703: str = "iter22-jra-cb-residual-703-v8"


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


def _three_horse_entries() -> list[dict[str, object]]:
    return [
        {"ketto_toroku_bango": "9001", "umaban": 1, "feature_a": 0.1, "feature_b": 0.4},
        {"ketto_toroku_bango": "9002", "umaban": 2, "feature_a": 0.2, "feature_b": 0.5},
        {"ketto_toroku_bango": "9003", "umaban": 3, "feature_a": 0.3, "feature_b": 0.6},
    ]


FEATURE_NAMES: list[str] = ["feature_a", "feature_b"]


# ---------------------------------------------------------------------------
# init_member_pool


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
    _write_member_model_json(tmp_path, "jra", "703", JRA_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0 if "iter14" in model_path else 0.5)

    monkeypatch.setattr(
        "predict_lib.booster_pool.load_catboost_booster",
        fake_load,
        raising=False,
    )
    # The lazy import inside load_booster_from_path is
    # ``from catboost_adapter import load_catboost_booster`` — install a stub
    # module on sys.modules so it resolves to our fake without the native
    # CatBoost runtime.
    from types import ModuleType

    class _FakeAdapter(ModuleType):
        load_catboost_booster: object

    fake_module = _FakeAdapter("catboost_adapter")
    fake_module.load_catboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "catboost_adapter", fake_module)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is True
    assert pool.has(ITER22_RESIDUAL_703) is True
    assert pool.model_versions() == (JRA_FALLBACK_MODEL_VERSION, ITER22_RESIDUAL_703)


def test_init_member_pool_empty_when_no_registry_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Categories with no registered ensembles (NAR / Ban-ei) -> empty pool."""
    pool_nar = init_member_pool(tmp_path, "nar")
    pool_banei = init_member_pool(tmp_path, "ban-ei")

    assert pool_nar.model_versions() == ()
    assert pool_banei.model_versions() == ()


def test_init_member_pool_skips_when_manifest_missing(tmp_path: Path) -> None:
    """Registry entry but no manifest on disk -> empty pool (no booster ask)."""
    pool = init_member_pool(tmp_path, "jra")

    assert pool.model_versions() == ()


def test_init_member_pool_filters_out_missing_member_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest lists two members but only one is on disk -> pool has only the
    one whose model.json exists. The downstream scoring path will detect the
    missing one per-race and fall back."""
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    # Only the residual member exists on disk.
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

    from types import ModuleType

    class _FakeAdapter(ModuleType):
        load_catboost_booster: object

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.5)

    fake_module = _FakeAdapter("catboost_adapter")
    fake_module.load_catboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "catboost_adapter", fake_module)

    pool = init_member_pool(tmp_path, "jra")

    assert pool.has(ITER22_RESIDUAL_703) is True
    assert pool.has(JRA_FALLBACK_MODEL_VERSION) is False


def test_init_member_pool_skips_other_categories_registry_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iterating the registry, entries for other categories are skipped — the
    pool requested for ``category`` only loads its own members. Confirms the
    `cat != category` guard inside the loop."""
    monkeypatch.setattr(
        per_class,
        "PER_CLASS_MODEL_VERSIONS",
        {
            ("jra", "703"): JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
            ("nar", "703"): "iter23-nar-xgb-ensemble-703-v8",
        },
    )
    _write_manifest(
        tmp_path,
        "jra",
        "703",
        JRA_CLASS_703_ENSEMBLE_MODEL_VERSION,
        _canonical_703_payload(),
    )
    _write_member_model_json(tmp_path, "jra", "703", JRA_FALLBACK_MODEL_VERSION)
    _write_member_model_json(tmp_path, "jra", "703", ITER22_RESIDUAL_703)

    from types import ModuleType

    class _FakeAdapter(ModuleType):
        load_catboost_booster: object

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(0.0)

    fake_module = _FakeAdapter("catboost_adapter")
    fake_module.load_catboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "catboost_adapter", fake_module)

    pool = init_member_pool(tmp_path, "jra")

    # NAR's registered entry never produced a manifest load and never inflated
    # the JRA pool.
    assert pool.model_versions() == (JRA_FALLBACK_MODEL_VERSION, ITER22_RESIDUAL_703)


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
            JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0),
            ITER22_RESIDUAL_703: _StubBooster(1.0),
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


# ---------------------------------------------------------------------------
# score_race_with_resolution — failure fallback paths


def test_score_race_with_resolution_falls_back_when_member_missing() -> None:
    """One ensemble member missing from the pool -> single-model fallback path
    with the global model_version label and ``member-missing:<mv>`` reason."""
    ensemble = _two_member_ensemble()
    # Only the iter14 member is in the pool; iter22 is missing.
    pool = BoosterPool(boosters={JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0)})
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
            JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0),
            ITER22_RESIDUAL_703: _RaisingBooster(),
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
            JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0),
            ITER22_RESIDUAL_703: _WrongLengthBooster(),
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
            JRA_FALLBACK_MODEL_VERSION: _WrongLengthBooster(),
            ITER22_RESIDUAL_703: _WrongLengthBooster(),
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
            JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0),
            ITER22_RESIDUAL_703: _StubBooster(1.0),
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
            JRA_FALLBACK_MODEL_VERSION: _StubBooster(0.0),
            ITER22_RESIDUAL_703: _StubBooster(0.0),
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
        resolution="iter12-nar-xgb-hpo-v8",
        race_id="nar:2026:0605:30:11",
        entries=_three_horse_entries(),
        feature_names=FEATURE_NAMES,
        architecture="xgboost",
        pool=pool,
        fallback_booster=fallback,
        fallback_model_version="iter12-nar-xgb-hpo-v8",
    )

    assert outcome.model_version == "iter12-nar-xgb-hpo-v8"
    assert outcome.fallback_reason is None
    assert outcome.scores == [0.2, 1.2, 2.2]
