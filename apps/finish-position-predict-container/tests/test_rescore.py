"""Unit tests for ``predict_lib.rescore``.

Covers race-scope matching (both-None wildcard / single-side / mismatch /
zero-pad normalization), scope filtering (matching subset / empty result), and
fresh-snapshot application (snapshot present overwrites / missing race falls
back / unknown umaban falls back).  All pure — fetch results are passed in as
plain dicts, no HTTP.
"""

from __future__ import annotations

import math

import pytest

from predict_lib.late_binding import OddsSnapshot
from predict_lib.rescore import (
    PostWeightValidationError,
    RaceFreshSnapshot,
    RaceScope,
    apply_fresh_snapshots,
    filter_post_weight_active_runners,
    filter_races_by_scope,
    race_matches_scope,
    race_scope_from_target_race,
    validate_post_weight_snapshots,
)

# ---------------------------------------------------------------------------
# race_matches_scope
# ---------------------------------------------------------------------------

_RACE_ID_NAR_44_01 = "nar:2026:0619:44:01"
_RACE_ID_NAR_44_02 = "nar:2026:0619:44:02"
_RACE_ID_NAR_30_01 = "nar:2026:0619:30:01"


def test_race_matches_scope_both_none_is_wildcard() -> None:
    assert race_matches_scope(_RACE_ID_NAR_44_01, RaceScope()) is True


def test_race_matches_scope_keibajo_only_match() -> None:
    assert race_matches_scope(_RACE_ID_NAR_44_01, RaceScope(keibajo_code="44")) is True


def test_race_matches_scope_keibajo_only_mismatch() -> None:
    assert race_matches_scope(_RACE_ID_NAR_30_01, RaceScope(keibajo_code="44")) is False


def test_race_matches_scope_full_match() -> None:
    scope = RaceScope(keibajo_code="44", race_bango="01")
    assert race_matches_scope(_RACE_ID_NAR_44_01, scope) is True


def test_race_matches_scope_race_bango_mismatch() -> None:
    scope = RaceScope(keibajo_code="44", race_bango="01")
    assert race_matches_scope(_RACE_ID_NAR_44_02, scope) is False


def test_race_matches_scope_zero_pad_race_bango() -> None:
    scope = RaceScope(keibajo_code="44", race_bango="1")
    assert race_matches_scope(_RACE_ID_NAR_44_01, scope) is True


def test_race_matches_scope_zero_pad_keibajo() -> None:
    scope = RaceScope(keibajo_code="44", race_bango="1")
    assert race_matches_scope("nar:2026:0619:44:01", scope) is True


def test_race_matches_scope_blank_keibajo_is_wildcard() -> None:
    assert race_matches_scope(_RACE_ID_NAR_44_01, RaceScope(keibajo_code="  ")) is True


def test_race_matches_scope_race_bango_only_match() -> None:
    assert race_matches_scope(_RACE_ID_NAR_44_01, RaceScope(race_bango="1")) is True


def test_race_matches_scope_race_bango_only_mismatch() -> None:
    assert race_matches_scope(_RACE_ID_NAR_44_02, RaceScope(race_bango="01")) is False


# ---------------------------------------------------------------------------
# filter_races_by_scope
# ---------------------------------------------------------------------------


def test_filter_races_by_scope_keeps_matching_only() -> None:
    races: dict[str, list[dict[str, object]]] = {
        _RACE_ID_NAR_44_01: [{"umaban": 1}],
        _RACE_ID_NAR_44_02: [{"umaban": 1}],
        _RACE_ID_NAR_30_01: [{"umaban": 1}],
    }
    result = filter_races_by_scope(races, RaceScope(keibajo_code="44"))
    assert sorted(result.keys()) == [_RACE_ID_NAR_44_01, _RACE_ID_NAR_44_02]


def test_filter_races_by_scope_single_race() -> None:
    races: dict[str, list[dict[str, object]]] = {
        _RACE_ID_NAR_44_01: [{"umaban": 1}],
        _RACE_ID_NAR_44_02: [{"umaban": 1}],
    }
    result = filter_races_by_scope(races, RaceScope(keibajo_code="44", race_bango="02"))
    assert list(result.keys()) == [_RACE_ID_NAR_44_02]


def test_filter_races_by_scope_empty_when_no_match() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [{"umaban": 1}]}
    result = filter_races_by_scope(races, RaceScope(keibajo_code="99"))
    assert result == {}


def test_filter_races_by_scope_wildcard_keeps_all() -> None:
    races: dict[str, list[dict[str, object]]] = {
        _RACE_ID_NAR_44_01: [{"umaban": 1}],
        _RACE_ID_NAR_30_01: [{"umaban": 1}],
    }
    result = filter_races_by_scope(races, RaceScope())
    assert sorted(result.keys()) == [_RACE_ID_NAR_30_01, _RACE_ID_NAR_44_01]


# ---------------------------------------------------------------------------
# race_scope_from_target_race
# ---------------------------------------------------------------------------


def test_race_scope_from_target_race_splits_keibajo_and_bango() -> None:
    scope = race_scope_from_target_race("05:11")
    assert scope == RaceScope(keibajo_code="05", race_bango="11")


def test_race_scope_from_target_race_keeps_unpadded_parts() -> None:
    scope = race_scope_from_target_race("5:1")
    assert scope == RaceScope(keibajo_code="5", race_bango="1")
    races: dict[str, list[dict[str, object]]] = {
        _RACE_ID_NAR_44_01: [{"umaban": 1}],
        _RACE_ID_NAR_44_02: [{"umaban": 2}],
    }
    filtered = filter_races_by_scope(races, race_scope_from_target_race("44:1"))
    assert list(filtered.keys()) == [_RACE_ID_NAR_44_01]


# ---------------------------------------------------------------------------
# apply_fresh_snapshots
# ---------------------------------------------------------------------------


def _entry(umaban: int) -> dict[str, object]:
    return {
        "keibajo_code": "44",
        "race_bango": "01",
        "umaban": umaban,
        "shusso_tosu": 12,
        "weight_avg_5": 450.0,
    }


def test_apply_fresh_snapshots_overwrites_odds_score() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(3)]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["odds_score"] == math.log(4.5) / math.log(300)


def test_apply_fresh_snapshots_overwrites_weight_diff() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(3)]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["weight_diff_from_avg"] == 8.0


def test_apply_fresh_snapshots_missing_race_uses_median() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(3)]}
    result = apply_fresh_snapshots(races, {}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["odds_score"] == 0.5048


def test_apply_fresh_snapshots_missing_race_weight_diff_none() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(3)]}
    result = apply_fresh_snapshots(races, {}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["weight_diff_from_avg"] is None


def test_apply_fresh_snapshots_unknown_umaban_uses_median() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(9)]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["odds_score"] == 0.5048


def test_apply_fresh_snapshots_unknown_umaban_weight_diff_none() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [_entry(9)]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["weight_diff_from_avg"] is None


def test_apply_fresh_snapshots_entry_without_umaban_uses_median() -> None:
    entry: dict[str, object] = {
        "keibajo_code": "44",
        "race_bango": "01",
        "shusso_tosu": 12,
        "weight_avg_5": 450.0,
    }
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [entry]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["odds_score"] == 0.5048


def test_apply_fresh_snapshots_entry_without_race_key_uses_median() -> None:
    entry: dict[str, object] = {"umaban": 3, "shusso_tosu": 12, "weight_avg_5": 450.0}
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [entry]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["odds_score"] == 0.5048


def test_apply_fresh_snapshots_preserves_early_column() -> None:
    entry = _entry(3)
    entry["jockey_win_rate"] = 0.18
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: [entry]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("44", "01"): snapshot}, "nar")
    assert result[_RACE_ID_NAR_44_01][0]["jockey_win_rate"] == 0.18


def test_apply_fresh_snapshots_normalizes_unpadded_race_key() -> None:
    entry: dict[str, object] = {
        "keibajo_code": "4",
        "race_bango": "1",
        "umaban": 3,
        "shusso_tosu": 12,
        "weight_avg_5": 450.0,
    }
    races: dict[str, list[dict[str, object]]] = {"nar:2026:0619:04:01": [entry]}
    snapshot = RaceFreshSnapshot(
        odds_by_umaban={3: OddsSnapshot(4.5, 2)},
        bataiju_by_umaban={3: 458.0},
    )
    result = apply_fresh_snapshots(races, {("04", "01"): snapshot}, "nar")
    assert result["nar:2026:0619:04:01"][0]["weight_diff_from_avg"] == 8.0


def test_validate_post_weight_snapshots_accepts_exact_runner_set() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1), _entry(3)]}
    snapshots = {
        ("44", "01"): RaceFreshSnapshot(odds_by_umaban={}, bataiju_by_umaban={1: 447.0, 3: 458.0})
    }
    validate_post_weight_snapshots(races, snapshots)


def test_filter_post_weight_active_runners_removes_japanese_canceled_runner() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1), _entry(2), _entry(3)]}

    result = filter_post_weight_active_runners(races, (1, 3), (2,))

    assert result == {_RACE_ID_NAR_44_01: [_entry(1), _entry(3)]}


def test_filter_post_weight_active_runners_rejects_missing_active_cache_runner() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1), _entry(2)]}

    with pytest.raises(
        PostWeightValidationError,
        match=r"post-weight entry snapshot mismatch: race=44:01 missing=\[3\] unexpected=\[\]",
    ):
        filter_post_weight_active_runners(races, (1, 3), (2,))


def test_filter_post_weight_active_runners_rejects_unclassified_cache_runner() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1), _entry(2), _entry(3)]}

    with pytest.raises(
        PostWeightValidationError,
        match=r"post-weight entry snapshot mismatch: race=44:01 missing=\[\] unexpected=\[3\]",
    ):
        filter_post_weight_active_runners(races, (1,), (2,))


def test_validate_post_weight_snapshots_rejects_missing_runner_weight() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1), _entry(3)]}
    snapshots = {("44", "01"): RaceFreshSnapshot(odds_by_umaban={}, bataiju_by_umaban={1: 447.0})}
    with pytest.raises(
        PostWeightValidationError,
        match=r"post-weight runner set mismatch: race=44:01 missing=\[3\] unexpected=\[\]",
    ):
        validate_post_weight_snapshots(races, snapshots)


def test_validate_post_weight_snapshots_rejects_unexpected_runner_weight() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1)]}
    snapshots = {
        ("44", "01"): RaceFreshSnapshot(odds_by_umaban={}, bataiju_by_umaban={1: 447.0, 9: 499.0})
    }
    with pytest.raises(
        PostWeightValidationError,
        match=r"post-weight runner set mismatch: race=44:01 missing=\[\] unexpected=\[9\]",
    ):
        validate_post_weight_snapshots(races, snapshots)


def test_validate_post_weight_snapshots_rejects_missing_race_snapshot() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1)]}
    with pytest.raises(PostWeightValidationError, match="post-weight race set mismatch"):
        validate_post_weight_snapshots(races, {})


def test_validate_post_weight_snapshots_rejects_non_positive_weight() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(1)]}
    snapshots = {("44", "01"): RaceFreshSnapshot(odds_by_umaban={}, bataiju_by_umaban={1: 0.0})}
    with pytest.raises(PostWeightValidationError, match="post-weight value invalid: race=44:01"):
        validate_post_weight_snapshots(races, snapshots)


def test_validate_post_weight_snapshots_rejects_invalid_cached_umaban() -> None:
    races = {_RACE_ID_NAR_44_01: [_entry(0)]}
    with pytest.raises(
        PostWeightValidationError, match="post-weight runner set invalid: race=44:01"
    ):
        validate_post_weight_snapshots(races, {})


def test_validate_post_weight_snapshots_rejects_empty_cached_race() -> None:
    races: dict[str, list[dict[str, object]]] = {_RACE_ID_NAR_44_01: []}
    with pytest.raises(PostWeightValidationError, match="post-weight runner set empty: race=44:01"):
        validate_post_weight_snapshots(races, {})
