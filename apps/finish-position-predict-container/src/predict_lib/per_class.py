"""Per-class JRA model routing (Phase B of the per-class architecture pivot).

The v8 production deploy (JRA=iter14-jra-cb-pacestyle-course-v8, NAR=iter12-nar-
xgb-hpo-v8) is a single global model per category. The per-class architecture
adds an optional second axis — ``kyoso_joken_code`` (race class) — so that future
per-class winners can be activated piecemeal without disturbing classes that
have no per-class winner yet.

Routing rules:

* ``PER_CLASS_MODEL_VERSIONS`` maps ``(category, kyoso_joken_code)`` to a
  registered per-class ``model_version`` string. An entry is added ONLY when a
  per-class model has beaten the category-global fallback (iter 14 for JRA) on
  its own subset; an unmapped class falls back to the category-global model.
* ``PER_CLASS_ENABLED_CATEGORIES`` lists the categories that participate in the
  per-class architecture. NAR / Ban-ei are intentionally excluded — neither has
  an actionable per-class plan yet — so they always return the category-global
  model regardless of ``kyoso_joken_code``.

As of 2026-06-05 v8 iter 20 confirmed all six candidate per-class JRA models
(005 / 010 / 016 / 703 / 701 / other) lose to iter 14 globally on their own
subsets, so ``PER_CLASS_MODEL_VERSIONS`` is intentionally empty. The container
runs with identical behaviour to the pre-per-class state until a per-class
winner is registered — see ``docs/finish-position-accuracy/runbook/PER_CLASS_ROUTING.md``.
"""

from __future__ import annotations

from typing import Final

from .model_meta import Category, model_version_for

# Currently empty — no per-class model has beaten iter 14 yet. Future iters
# that produce a per-class winner add their model_version here keyed by
# (category, kyoso_joken_code). Container code falls back to
# MODEL_VERSION_BY_CATEGORY[category] when a class is unmapped.
PER_CLASS_MODEL_VERSIONS: Final[dict[tuple[Category, str], str]] = {}

# Categories that participate in per-class routing. NAR / Ban-ei are excluded
# so their ``resolve_per_class_model_version`` always returns the category-global
# model — adding them here would silently change routing behaviour, so the
# allowlist is the single switch.
PER_CLASS_ENABLED_CATEGORIES: Final[frozenset[Category]] = frozenset({"jra"})


def is_per_class_enabled_for(category: Category) -> bool:
    """Return True when ``category`` participates in per-class routing."""
    return category in PER_CLASS_ENABLED_CATEGORIES


def resolve_per_class_model_version(
    category: Category,
    kyoso_joken_code: str | None,
) -> str:
    """Return per-class model_version if registered, else category fallback.

    Falls back to ``model_version_for(category)`` when:

    * the category is not per-class enabled (NAR / Ban-ei),
    * the race has no ``kyoso_joken_code`` (e.g. the column was NULL in PG and
      the feature build emitted ``None``), or
    * the class code has no registered per-class winner yet.

    All three branches map to the SAME global model_version so the caller can
    treat the return value as an opaque label and is never accidentally routed
    to a non-existent per-class booster.
    """
    if not is_per_class_enabled_for(category):
        return model_version_for(category)
    if kyoso_joken_code is None:
        return model_version_for(category)
    return PER_CLASS_MODEL_VERSIONS.get(
        (category, kyoso_joken_code),
        model_version_for(category),
    )


def per_class_codes_for(category: Category) -> tuple[str, ...]:
    """Return the registered per-class codes for ``category`` in sorted order.

    Used by callers that need to pre-load per-class boosters at startup. Returns
    an empty tuple for disabled categories AND for enabled categories that have
    no registered per-class winners yet.
    """
    if not is_per_class_enabled_for(category):
        return ()
    codes = {code for cat, code in PER_CLASS_MODEL_VERSIONS if cat == category}
    return tuple(sorted(codes))
