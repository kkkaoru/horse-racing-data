"""Unit tests for ``predict_lib.rescore``.

Covers race-scope matching (both-None wildcard / single-side / mismatch /
zero-pad normalization), scope filtering (matching subset / empty result), and
fresh-snapshot application (snapshot present overwrites / missing race falls
back / unknown umaban falls back).  All pure — fetch results are passed in as
plain dicts, no HTTP.
"""

from __future__ import annotations

import math

from predict_lib.late_binding import OddsSnapshot
from predict_lib.rescore import (
    RaceFreshSnapshot,
    RaceScope,
    apply_fresh_snapshots,
    filter_races_by_scope,
    race_matches_scope,
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
