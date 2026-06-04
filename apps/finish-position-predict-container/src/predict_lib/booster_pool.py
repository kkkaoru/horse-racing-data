"""Multi-booster loading + lookup for ensemble routing (Phase B-2C).

Manages a pool of CatBoost JSON boosters loaded at startup. Lookup by
``model_version`` string at scoring time. Per-class ensemble routing uses
this to get all member models in one shot.

For single-model routing (iter14 fallback), the existing booster loading
path (``catboost_adapter.load_catboost_booster``) is unchanged — this pool is
only used when a per-class ensemble is resolved. The booster objects stored
in the pool implement the ``BoosterLike`` protocol (same surface the scorer
consumes), so the pool stays free of native imports at type-check time —
the CatBoost runtime is imported lazily inside ``load_booster_from_path``,
mirroring ``catboost_adapter``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .scorer import BoosterLike

CATBOOST_MODEL_FORMAT: str = "json"


@dataclass(frozen=True)
class BoosterPool:
    """Holds loaded boosters keyed by ``model_version``.

    The ``boosters`` dict is intentionally a plain ``dict`` so the pool can be
    constructed once at startup and shared across requests; lookups via
    :meth:`get` / :meth:`has` are read-only. ``frozen=True`` prevents the
    field itself from being rebound — callers should not mutate the dict
    after construction.
    """

    boosters: dict[str, BoosterLike]

    def get(self, model_version: str) -> BoosterLike | None:
        """Return the booster for ``model_version`` or ``None`` if missing."""
        return self.boosters.get(model_version)

    def has(self, model_version: str) -> bool:
        """Return True when ``model_version`` is loaded in the pool."""
        return model_version in self.boosters

    def model_versions(self) -> tuple[str, ...]:
        """Return loaded ``model_version`` labels in sorted order (stable)."""
        return tuple(sorted(self.boosters))


def load_booster_from_path(model_json_path: Path) -> BoosterLike:
    """Load a single CatBoost JSON model from ``model_json_path``.

    Raises ``FileNotFoundError`` when the path does not exist so the caller can
    decide whether a missing member is fatal (single-shot deploy) or
    fall-back-safe (ensemble with optional members). The CatBoost runtime is
    imported lazily so ``predict_lib`` stays free of native imports at type-
    check time, mirroring ``catboost_adapter.load_catboost_booster``.
    """
    if not model_json_path.exists():
        message = f"booster missing: {model_json_path}"
        raise FileNotFoundError(message)
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
        candidate = base / model_version / "model.json"
        if candidate.exists():
            found[model_version] = candidate
    return found


def build_pool_from_paths(paths_by_version: dict[str, Path]) -> BoosterPool:
    """Load every booster in ``paths_by_version`` into a fresh ``BoosterPool``.

    All paths must exist — :func:`load_booster_from_path` raises
    ``FileNotFoundError`` otherwise. Use :func:`discover_member_models` first
    to filter to existing paths when missing members should be tolerated.
    """
    boosters: dict[str, BoosterLike] = {
        model_version: load_booster_from_path(path)
        for model_version, path in paths_by_version.items()
    }
    return BoosterPool(boosters=boosters)
