"""Tracked declaration of the ``RUNNING_STYLE_CELL_ROUTING_JSON`` Cloudflare
Worker secret, for artifact-selection preflight verification only.

That secret is consumed by ``apps/sync-realtime-data``'s
``running-style-cell-router.ts`` (a different Worker; this app does not, and
must not, edit that package) and can select an arbitrary
``variants[*].modelKey`` R2 object per category -- including a category this
package's verifier previously never looked at (``ban-ei``), since the config
schema is ``Partial<Record<"jra" | "nar" | "ban-ei", ...>>`` even though only
JRA and NAR have a live running-style artifact today. Like
``NAR_TRANSFORMER_BLEND_ENABLED`` (see ``deploy_flags.py``), the secret is
write-only from the CLI, so no local process can read back its live value.

``running_style_cell_routing.json`` is the single tracked, git-diffable
declaration this package's verifier walks instead of a hardcoded jra/nar pair.
Whoever runs ``wrangler secret put RUNNING_STYLE_CELL_ROUTING_JSON`` MUST
update this file in the SAME commit (see DEPLOY.md); a newly cell-routed
variant then automatically joins the verifier's selector closure -- for ANY of
jra / nar / ban-ei -- instead of silently escaping verification the way a
hardcoded jra/nar-only closure would.

Schema mirrors ``RunningStyleCellRoutingConfig`` in
``apps/sync-realtime-data/src/running-style-cell-router.ts``: a mapping of
category -> ``{defaultVariantId, rules: [{conditions, variantId}], variants:
{variantId: {modelKey}}}``. This module only needs the REACHABLE set of
``modelKey`` values (the default variant plus every rule-referenced variant)
because verification never evaluates a specific race's per-race condition
match -- it only needs to know which artifacts *could* be selected by some
race, so every referenced variant must be manifested and byte-correct.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

RUNNING_STYLE_ROUTING_PATH: Final[Path] = (
    Path(__file__).parent / "running_style_cell_routing.json"
)
RUNNING_STYLE_ROUTING_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar", "ban-ei"})


class RunningStyleRoutingValidationError(ValueError):
    """Raised when the tracked running-style routing declaration is malformed."""


@dataclass(frozen=True)
class RunningStyleVariant:
    model_key: str


@dataclass(frozen=True)
class RunningStyleCategoryRouting:
    default_variant_id: str
    rule_variant_ids: tuple[str, ...]
    variants: Mapping[str, RunningStyleVariant] = field(default_factory=dict)

    def reachable_model_keys(self) -> frozenset[str]:
        """Every ``modelKey`` some race could resolve to for this category.

        Raises when the default variant or any rule references a variant id
        absent from ``variants`` -- the same malformed-config condition
        ``resolveRunningStyleCellRoute`` raises on at serve time -- so a
        broken tracked declaration fails the preflight closed instead of
        silently under-selecting.
        """
        referenced = frozenset({self.default_variant_id, *self.rule_variant_ids})
        missing = sorted(referenced - self.variants.keys())
        if missing:
            raise RunningStyleRoutingValidationError(
                f"running_style_cell_routing.json references undefined variant ids: {missing}"
            )
        return frozenset(self.variants[variant_id].model_key for variant_id in referenced)


def _as_mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise RunningStyleRoutingValidationError(f"{context} must be an object")
    return {str(key): item for key, item in value.items()}


def _as_sequence(value: object, context: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise RunningStyleRoutingValidationError(f"{context} must be an array")
    return value


def _require_non_empty_string(payload: Mapping[str, object], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RunningStyleRoutingValidationError(f"{context}.{key} must be a non-empty string")
    return value


def _parse_variant(value: object, context: str) -> RunningStyleVariant:
    payload = _as_mapping(value, context)
    return RunningStyleVariant(model_key=_require_non_empty_string(payload, "modelKey", context))


def _parse_rule_variant_id(value: object, index: int, context: str) -> str:
    rule_context = f"{context}[{index}]"
    payload = _as_mapping(value, rule_context)
    return _require_non_empty_string(payload, "variantId", rule_context)


def _parse_category_routing(value: object, category: str) -> RunningStyleCategoryRouting:
    context = f"running_style_cell_routing.json[{category}]"
    payload = _as_mapping(value, context)
    default_variant_id = _require_non_empty_string(payload, "defaultVariantId", context)
    rules = _as_sequence(payload.get("rules", []), f"{context}.rules")
    rule_variant_ids = tuple(
        _parse_rule_variant_id(rule, index, f"{context}.rules") for index, rule in enumerate(rules)
    )
    variants_payload = _as_mapping(payload.get("variants", {}), f"{context}.variants")
    variants = {
        variant_id: _parse_variant(spec, f"{context}.variants.{variant_id}")
        for variant_id, spec in variants_payload.items()
    }
    return RunningStyleCategoryRouting(
        default_variant_id=default_variant_id,
        rule_variant_ids=rule_variant_ids,
        variants=variants,
    )


def load_running_style_cell_routing(
    path: Path = RUNNING_STYLE_ROUTING_PATH,
) -> dict[str, RunningStyleCategoryRouting]:
    """Load the tracked routing declaration; ``{}`` means no live cell routing."""
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise RunningStyleRoutingValidationError(
            f"running_style_cell_routing.json not found: {path}"
        ) from error
    try:
        payload: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RunningStyleRoutingValidationError(
            f"invalid running_style_cell_routing.json: {error.msg}"
        ) from error
    root = _as_mapping(payload, "running_style_cell_routing.json")
    unknown = sorted(frozenset(root) - RUNNING_STYLE_ROUTING_CATEGORIES)
    if unknown:
        raise RunningStyleRoutingValidationError(
            f"running_style_cell_routing.json has unsupported categories: {unknown}"
        )
    return {category: _parse_category_routing(entry, category) for category, entry in root.items()}
