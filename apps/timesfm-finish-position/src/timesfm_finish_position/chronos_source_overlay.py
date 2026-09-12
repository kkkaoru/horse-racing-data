"""Apply explicit, fingerprint-checked result corrections without changing identity."""

import re
from collections.abc import Mapping

ALLOWED_FIELDS: dict[str, str] = {
    "kakutei_chakujun": r"[0-9]{2}",
    "ijo_kubun_code": r"[0-7]",
    "tansho_odds": r"[0-9]{4}",
    "shusso_tosu": r"[0-9]{2}",
    "nyusen_tosu": r"[0-9]{2}",
}


def apply_source_overlay(
    row: Mapping[str, str | None],
    *,
    expected: Mapping[str, str | None],
    replacement: Mapping[str, str],
) -> dict[str, str | None]:
    """Require exact prior fields and preserve IDs and original vendor version.

    The caller must establish authoritative full-roster evidence first and retain
    a before/after ledger. This function never chooses which runners are eligible.
    """
    if dict(row) != dict(expected):
        raise ValueError("Source snapshot changed before official overlay")
    if not replacement or not replacement.keys() <= ALLOWED_FIELDS.keys():
        raise ValueError("Unsupported official overlay fields")
    if not replacement.keys() <= row.keys():
        raise ValueError("Official overlay cannot add source columns")
    for key, value in replacement.items():
        if re.fullmatch(ALLOWED_FIELDS[key], value) is None:
            raise ValueError(f"Invalid official overlay value: {key}")
    return {**row, **replacement}
