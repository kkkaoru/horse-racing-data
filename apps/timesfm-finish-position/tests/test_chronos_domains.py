"""Historical Ban-ei venues remain isolated from both flat racing domains."""

import numpy as np
import pytest

from timesfm_finish_position.chronos_domains import (
    domain_contexts,
    domain_labels,
    parse_training_domain,
)


def test_label_domains_do_not_overlap() -> None:
    races = np.array(["jra:r", "nar:r", "nar:old", "nar:now", "jra:foreign"])
    venues = np.array(["05", "50", "81", "83", "C7"])
    np.testing.assert_array_equal(
        domain_labels(races, venues, "jra"), [True, False, False, False, False]
    )
    np.testing.assert_array_equal(
        domain_labels(races, venues, "nar"), [False, True, False, False, False]
    )
    np.testing.assert_array_equal(
        domain_labels(races, venues, "banei"), [False, False, True, True, False]
    )


def test_context_transfer_and_old_banei_venues() -> None:
    venues = np.array(["05", "50", "81", "82", "83", "84"])
    np.testing.assert_array_equal(
        domain_contexts(venues, "jra"), [True, True, False, False, False, False]
    )
    np.testing.assert_array_equal(
        domain_contexts(venues, "nar"), [True, True, False, False, False, False]
    )
    np.testing.assert_array_equal(
        domain_contexts(venues, "banei"), [False, False, True, True, True, True]
    )


def test_invalid_domain_and_alignment() -> None:
    assert parse_training_domain("nar") == "nar"
    with pytest.raises(ValueError, match="Training domain"):
        parse_training_domain("mixed")
    with pytest.raises(ValueError, match="align"):
        domain_labels(np.array(["jra:r"]), np.array([], dtype=str), "jra")
