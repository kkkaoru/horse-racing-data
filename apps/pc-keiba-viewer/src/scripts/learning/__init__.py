"""Continuous Walk-Forward learning loop package.

Key symbols from :mod:`learning.feature_registry` are re-exported lazily via
:pep:`562` ``__getattr__`` so that ``import learning`` does not eagerly import
``duckdb`` (eager import here corrupts the ``_duckdb`` C-extension during full
pytest collection). Submodules remain importable directly, e.g.
``from learning.feature_registry import FeatureRegistry``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from learning.feature_registry import (
        INVERSE_APPROACH_TYPES as INVERSE_APPROACH_TYPES,
        FeatureEntry as FeatureEntry,
        FeatureRegistry as FeatureRegistry,
    )

__all__ = ["INVERSE_APPROACH_TYPES", "FeatureEntry", "FeatureRegistry"]


def __getattr__(name: str) -> object:
    if name in __all__:
        from learning import feature_registry

        return getattr(feature_registry, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
