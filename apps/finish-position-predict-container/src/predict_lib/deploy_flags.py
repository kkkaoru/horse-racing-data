"""Tracked deploy-intent flags for artifact-selection preflight verification.

``NAR_TRANSFORMER_BLEND_ENABLED`` is a `finish-position-cron` Cloudflare Worker
*secret* (``wrangler secret put``): it is write-only from the CLI -- Cloudflare
never returns a previously-set secret's value, so no local process can read
back what is actually live. ``predict_lib.model_meta._env_flag`` reads this
name from ``os.environ`` as the container's OWN runtime seam (correct there --
the Cloudflare Worker injects the real secret into the container's env at
start()), but that same env-var read is the WRONG default source for a local
preflight verifier: an ambient shell export (or, far more commonly, its
complete ABSENCE) has no relationship to what is actually configured on
Cloudflare, so trusting it silently lets local verification diverge from what
is live -- e.g. skipping verification of the NAR transformer artifact bundle
entirely because a developer's shell happens not to have the variable set.

``deploy_flags.json`` is the single tracked, git-diffable, PR-reviewable
declaration this package's verifier consults instead. Whoever runs
``wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED`` MUST update this file in
the SAME commit (see DEPLOY.md); the preflight then fails closed against a
deliberate, reviewed value rather than ambient process state. This does not
claim to read the live Cloudflare secret (that is impossible without a
Cloudflare API round trip this package does not perform) -- it converts an
accidental divergence source into a deliberate one that code review can catch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final

DEPLOY_FLAGS_PATH: Final[Path] = Path(__file__).parent / "deploy_flags.json"
_REQUIRED_KEYS: Final[frozenset[str]] = frozenset({"nar_transformer_blend_enabled"})


class DeployFlagsValidationError(ValueError):
    """Raised when the tracked deploy-flags declaration violates its contract."""


@dataclass(frozen=True)
class DeployFlags:
    nar_transformer_blend_enabled: bool


def _load_payload(path: Path) -> dict[str, object]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise DeployFlagsValidationError(f"deploy_flags.json not found: {path}") from error
    try:
        payload: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise DeployFlagsValidationError(f"invalid deploy_flags.json: {error.msg}") from error
    if not isinstance(payload, dict):
        raise DeployFlagsValidationError("deploy_flags.json must be an object")
    return {str(key): value for key, value in payload.items()}


def load_deploy_flags(path: Path = DEPLOY_FLAGS_PATH) -> DeployFlags:
    """Load the tracked deploy-intent declaration; raises on any malformed shape."""
    payload = _load_payload(path)
    keys = frozenset(payload)
    if keys != _REQUIRED_KEYS:
        missing = sorted(_REQUIRED_KEYS - keys)
        unexpected = sorted(keys - _REQUIRED_KEYS)
        raise DeployFlagsValidationError(
            f"deploy_flags.json keys differ: missing={missing}, unexpected={unexpected}"
        )
    value = payload["nar_transformer_blend_enabled"]
    if not isinstance(value, bool):
        raise DeployFlagsValidationError(
            "deploy_flags.json.nar_transformer_blend_enabled must be a boolean"
        )
    return DeployFlags(nar_transformer_blend_enabled=value)
