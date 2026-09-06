"""Canonical maximum-granularity signature for one served scoring branch."""

from __future__ import annotations

from dataclasses import dataclass

_SIGNATURE_VERSION = "v1"
_SEPARATOR = ";"
_ASSIGNMENT = "="


@dataclass(frozen=True, slots=True)
class ServedBranch:
    """Routing decisions that fully identify the score path served for a race."""

    routing_mode: str
    stage2_outcome: str
    stage2_model_version: str
    stage1_reason: str
    final_model_version: str

    def signature(self) -> str:
        values = (
            ("mode", self.routing_mode),
            ("stage2", self.stage2_outcome),
            ("stage2-model", self.stage2_model_version),
            ("stage1", self.stage1_reason),
            ("final", self.final_model_version),
        )
        for field, value in values:
            if not value or _SEPARATOR in value or _ASSIGNMENT in value:
                raise ValueError(f"Served branch {field} contains an invalid token")
        fields = _SEPARATOR.join(f"{field}{_ASSIGNMENT}{value}" for field, value in values)
        return f"{_SIGNATURE_VERSION}{_SEPARATOR}{fields}"
