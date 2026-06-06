"""Multi-booster loading + lookup for ensemble routing (Phase B-2C + Phase F).

Manages a pool of CatBoost / XGBoost JSON boosters loaded at startup. Lookup by
``model_version`` string at scoring time. Per-class ensemble routing uses this
to get all member models in one shot.

For single-model routing (iter14 JRA / iter12 NAR fallback), the existing
booster loading path (``catboost_adapter.load_catboost_booster`` /
``xgboost_adapter.load_xgboost_booster``) is unchanged — this pool is only used
when a per-class ensemble is resolved. The booster objects stored in the pool
implement the ``BoosterLike`` protocol (same surface the scorer consumes), so
the pool stays free of native imports at type-check time — the CatBoost /
XGBoost runtimes are imported lazily inside ``load_booster_from_path``,
mirroring ``catboost_adapter`` / ``xgboost_adapter``.

Phase F (2026-06-05) adds architecture-aware loading so NAR per-class
ensembles can blend the iter 12 XGBoost baseline with iter 30 CatBoost residual
members. Each pool entry carries its :class:`PoolBooster` (booster + arch)
record so the scoring layer knows which feature-matrix dtype to build for each
member (CatBoost = float64, XGBoost = float32-quantised per
``predict_lib.scorer.build_feature_row``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .model_meta import Architecture
from .scorer import BoosterLike

CATBOOST_MODEL_FORMAT: str = "json"
MODEL_JSON_FILE_NAME: str = "model.json"
FEATURE_NAMES_KEY: str = "feature_names"


@dataclass(frozen=True)
class PoolBooster:
    """One booster + the architecture used to score against it.

    The architecture is part of the pool record (not derived from the
    ``model_version`` string) so the same model_version could theoretically be
    re-loaded under a different arch without changing the lookup key — kept
    immutable + frozen so callers cannot mutate the binding after build.

    ``feature_names`` is the member's OWN ordered training feature list (read
    from the sibling ``metadata.json``), defaulting to the empty tuple for
    callers that have not wired it through yet. CatBoost / XGBoost score
    POSITIONALLY, so the scorer projects each member's entries onto exactly
    this name order — a member trained on 254 columns must NOT be scored with
    the 241-wide category-global list, which is the latent fallback bug this
    field fixes.
    """

    booster: BoosterLike
    architecture: Architecture
    feature_names: tuple[str, ...] = field(default=())


def load_member_feature_names(metadata_json_path: Path) -> tuple[str, ...]:
    """Read a member's ordered ``feature_names`` from its sibling metadata.json.

    The metadata.json lives next to every member ``model.json`` and carries the
    full ordered ``feature_names`` the member was trained on (verified: the
    order matches the CatBoost internal ``float_features`` order, so the scorer
    can project entries positionally). Raises loudly so a corrupt / missing
    sidecar is never silently swallowed into a wrong-width matrix:

    * ``FileNotFoundError`` — the metadata.json does not exist;
    * ``ValueError`` — the file is not valid JSON, the ``feature_names`` key is
      absent, or its value is not a list of strings.
    """
    if not metadata_json_path.exists():
        message = f"member metadata missing: {metadata_json_path}"
        raise FileNotFoundError(message)
    try:
        payload = json.loads(metadata_json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as decode_error:
        message = f"member metadata not valid JSON: {metadata_json_path}"
        raise ValueError(message) from decode_error
    if not isinstance(payload, dict):
        message = f"member metadata is not an object: {metadata_json_path}"
        raise ValueError(message)
    feature_names = payload.get(FEATURE_NAMES_KEY)
    if not isinstance(feature_names, list):
        message = f"member metadata missing feature_names: {metadata_json_path}"
        raise ValueError(message)
    if not all(isinstance(name, str) for name in feature_names):
        message = f"member feature_names not all strings: {metadata_json_path}"
        raise ValueError(message)
    return tuple(feature_names)


@dataclass(frozen=True)
class BoosterPool:
    """Holds loaded boosters keyed by ``model_version``.

    The ``boosters`` dict is intentionally a plain ``dict`` so the pool can be
    constructed once at startup and shared across requests; lookups via
    :meth:`get` / :meth:`has` / :meth:`get_record` are read-only.
    ``frozen=True`` prevents the field itself from being rebound — callers
    should not mutate the dict after construction.

    Each entry is a :class:`PoolBooster` so the scoring layer can route the
    member to the right feature-matrix dtype. :meth:`get` returns the
    ``BoosterLike`` only for backward compatibility with Phase B-2C callers;
    :meth:`get_record` is the architecture-aware accessor introduced in Phase F.
    """

    boosters: dict[str, PoolBooster]

    def get(self, model_version: str) -> BoosterLike | None:
        """Return the booster for ``model_version`` or ``None`` if missing."""
        record = self.boosters.get(model_version)
        if record is None:
            return None
        return record.booster

    def get_record(self, model_version: str) -> PoolBooster | None:
        """Return the booster + architecture for ``model_version`` or ``None``.

        Architecture-aware accessor — the ensemble scorer uses this so it can
        build a separate feature matrix per arch (CatBoost = float64, XGBoost =
        float32-quantised) when a mixed-arch NAR ensemble is resolved.
        """
        return self.boosters.get(model_version)

    def has(self, model_version: str) -> bool:
        """Return True when ``model_version`` is loaded in the pool."""
        return model_version in self.boosters

    def model_versions(self) -> tuple[str, ...]:
        """Return loaded ``model_version`` labels in sorted order (stable)."""
        return tuple(sorted(self.boosters))


def load_booster_from_path(
    model_json_path: Path, architecture: Architecture
) -> BoosterLike:
    """Load a single CatBoost / XGBoost JSON model from ``model_json_path``.

    Dispatches by ``architecture``: ``"catboost"`` -> CatBoost JSON (used by
    JRA per-class + NAR iter 30 residual CatBoost members); ``"xgboost"`` ->
    XGBoost JSON (used by the NAR iter 12 baseline carried as a member of NAR
    iter 30 ensembles). Raises ``FileNotFoundError`` when the path does not
    exist so the caller can decide whether a missing member is fatal
    (single-shot deploy) or fall-back-safe (ensemble with optional members).
    The native runtimes are imported lazily so ``predict_lib`` stays free of
    native imports at type-check time, mirroring the JRA-only Phase B-2C
    behaviour.
    """
    if not model_json_path.exists():
        message = f"booster missing: {model_json_path}"
        raise FileNotFoundError(message)
    if architecture == "xgboost":
        from xgboost_adapter import load_xgboost_booster

        return load_xgboost_booster(str(model_json_path))
    from catboost_adapter import load_catboost_booster

    return load_catboost_booster(str(model_json_path))


def discover_member_models(
    models_root: Path,
    category: str,
    kyoso_joken_code: str,
    member_model_versions: tuple[str, ...],
) -> dict[str, Path]:
    """Resolve on-disk paths for each member ``model_version``.

    Searches ``{models_root}/{category}/per-class/{kyoso_joken_code}/{mv}/model.json``
    for every entry in ``member_model_versions`` and returns the subset that
    actually exists. Missing members are silently skipped — the caller decides
    whether to abort or score the ensemble with the surviving members.
    """
    base = models_root / category / "per-class" / kyoso_joken_code
    found: dict[str, Path] = {}
    for model_version in member_model_versions:
        candidate = base / model_version / MODEL_JSON_FILE_NAME
        if candidate.exists():
            found[model_version] = candidate
    return found


def discover_baseline_member_model(
    models_root: Path,
    category: str,
    model_version: str,
) -> Path | None:
    """Resolve the on-disk path for a category-global baseline carried as a
    member of a per-class ensemble (e.g. ``iter12-nar-xgb-hpo-v8`` inside the
    iter 30 NAR ensembles).

    Searches ``{models_root}/{category}/{model_version}/model.json`` — the
    SAME path the category-global single-model loader uses — so the baseline
    booster is loaded exactly once when the daily predictor walks the per-class
    registry. Returns ``None`` when the file is absent.
    """
    candidate = models_root / category / model_version / MODEL_JSON_FILE_NAME
    if candidate.exists():
        return candidate
    return None


def build_pool_from_paths(
    paths_by_version: dict[str, Path],
    architecture_by_version: dict[str, Architecture],
    feature_names_by_version: dict[str, tuple[str, ...]] | None = None,
) -> BoosterPool:
    """Load every booster in ``paths_by_version`` into a fresh ``BoosterPool``.

    Each ``model_version`` MUST appear in ``architecture_by_version`` so the
    loader can dispatch to CatBoost vs XGBoost. All paths must exist —
    :func:`load_booster_from_path` raises ``FileNotFoundError`` otherwise. Use
    :func:`discover_member_models` first to filter to existing paths when
    missing members should be tolerated.

    ``feature_names_by_version`` carries each member's OWN ordered feature list
    (from its metadata.json) so the scorer can build a per-member matrix of the
    right width. A ``model_version`` absent from the map (or a ``None`` map)
    stores the empty tuple — the scorer then falls back to the caller-supplied
    global feature list, preserving the single-model fallback path.
    """
    names_by_version = feature_names_by_version or {}
    boosters: dict[str, PoolBooster] = {}
    for model_version, path in paths_by_version.items():
        architecture = architecture_by_version[model_version]
        booster = load_booster_from_path(path, architecture)
        boosters[model_version] = PoolBooster(
            booster=booster,
            architecture=architecture,
            feature_names=names_by_version.get(model_version, ()),
        )
    return BoosterPool(boosters=boosters)
