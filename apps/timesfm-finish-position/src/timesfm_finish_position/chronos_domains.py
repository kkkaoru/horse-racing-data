"""Separate JRA, NAR flat and Ban-ei labels, contexts and experiment namespaces."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

import numpy as np
import numpy.typing as npt

TrainingDomain = Literal["jra", "nar", "banei"]
DOMAINS: Mapping[str, TrainingDomain] = MappingProxyType(
    {"jra": "jra", "nar": "nar", "banei": "banei"}
)
BANEI_VENUES: tuple[str, ...] = ("81", "82", "83", "84")
JRA_VENUES: tuple[str, ...] = tuple(f"{number:02d}" for number in range(1, 11))


def parse_training_domain(value: object) -> TrainingDomain:
    if not isinstance(value, str) or value not in DOMAINS:
        raise ValueError("Training domain must be jra, nar or banei")
    return DOMAINS[value]


def domain_labels(
    race_ids: npt.NDArray[np.str_], venues: npt.NDArray[np.str_], domain: TrainingDomain
) -> npt.NDArray[np.bool_]:
    parse_training_domain(domain)
    if race_ids.shape != venues.shape:
        raise ValueError("Domain columns must align")
    banei = np.isin(venues, BANEI_VENUES)
    if domain == "jra":
        return np.char.startswith(race_ids, "jra:") & np.isin(venues, JRA_VENUES)
    nar = np.char.startswith(race_ids, "nar:")
    return nar & banei if domain == "banei" else nar & ~banei & (venues >= "30") & (venues <= "79")


def domain_contexts(venues: npt.NDArray[np.str_], domain: TrainingDomain) -> npt.NDArray[np.bool_]:
    """Flat racing may retain transfer history; never mix it with Ban-ei."""
    parse_training_domain(domain)
    banei = np.isin(venues, BANEI_VENUES)
    return banei if domain == "banei" else ~banei
