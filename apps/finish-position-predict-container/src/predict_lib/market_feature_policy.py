"""Win-odds / popularity ban for container-side trainers.

A copy of pc-keiba-viewer ``learning.feature_selection_policy`` (the source of
truth; this package cannot import it). pc-keiba-viewer's
tests/test_feature_selection_policy_container_parity.py fails if the two drift.
"""

from __future__ import annotations

import re
from typing import Final

MARKET_DEVIATION_FEATURES: Final[frozenset[str]] = frozenset(
    {"exotic_umaren_p2_rank_residual", "exotic_sanrenpuku_p3_rank_residual"}
)
BANNED_MARKET_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"tansho|ninki|popular|fav(_|ou?rite)|fukusho|wide|umatan|sanrentan|wakuren"
    r"|odds|market|^exotic_|form_market_edge"
)


def is_banned_market_feature(column: str) -> bool:
    """True when ``column`` is an odds/popularity input finish models must not use."""
    return column not in MARKET_DEVIATION_FEATURES and bool(BANNED_MARKET_PATTERN.search(column))
