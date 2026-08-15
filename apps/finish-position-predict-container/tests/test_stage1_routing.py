"""Tests for the JRA Stage-1 market-free gated-fallback routing module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from predict_lib.stage1_routing import (
    PREDICTED_SCORE_COLUMN_INDEX,
    STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV,
    STAGE1_ROUTING_PATH,
    Stage1CategoryConfig,
    Stage1GateDecision,
    Stage1RoutingValidationError,
    compute_predicted_score_stddev,
    extract_predicted_scores,
    is_score_spread_degraded,
    load_stage1_routing,
    preserved_odds_gate_enabled,
    race_has_fresh_odds,
    resolve_stage1_gate,
)
from predict_lib.upsert_sql import INSERT_COLUMNS

_VALID_JRA_CONFIG: dict[str, object] = {
    "enabled": True,
    "model_version": "jra-cb-stage1-marketfree235-2013",
    "feature_count": 235,
    "architecture": "catboost",
    "stddev_threshold": 0.3,
    "enable_stddev_safety_net": True,
}


def _write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "stage1_routing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# load_stage1_routing
# ---------------------------------------------------------------------------


def test_stage1_routing_path_points_at_tracked_file() -> None:
    assert STAGE1_ROUTING_PATH.name == "stage1_routing.json"
    assert STAGE1_ROUTING_PATH.exists()


def test_tracked_stage1_routing_loads_live_jra_config() -> None:
    """The real tracked file must parse and describe the live JRA fallback."""
    routing = load_stage1_routing()

    assert routing["jra"] == Stage1CategoryConfig(
        enabled=True,
        model_version="jra-cb-stage1-marketfree235-2013",
        feature_count=235,
        architecture="catboost",
        stddev_threshold=0.4,
        enable_stddev_safety_net=True,
    )


def test_tracked_stage1_routing_loads_live_nar_config() -> None:
    """The real tracked file must parse and describe the live NAR fallback.

    NAR runs freshness-gate-only: its Stage-2 is a within-race znorm blend whose
    within-race predicted_score stddev never collapses (measured floor ~0.36
    across 548 served races), so the stddev safety net cannot separate incident
    from healthy for NAR -- enable_stddev_safety_net is False and stddev_threshold
    is inert.
    """
    routing = load_stage1_routing()

    assert routing["nar"] == Stage1CategoryConfig(
        enabled=True,
        model_version="iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
        feature_count=184,
        architecture="xgboost",
        stddev_threshold=0.3,
        enable_stddev_safety_net=False,
    )


def test_load_stage1_routing_parses_full_shape(tmp_path: Path) -> None:
    path = _write(tmp_path, {"jra": _VALID_JRA_CONFIG})

    routing = load_stage1_routing(path)

    assert routing == {
        "jra": Stage1CategoryConfig(
            enabled=True,
            model_version="jra-cb-stage1-marketfree235-2013",
            feature_count=235,
            architecture="catboost",
            stddev_threshold=0.3,
            enable_stddev_safety_net=True,
        )
    }


def test_load_stage1_routing_empty_object_means_no_category_configured(tmp_path: Path) -> None:
    path = _write(tmp_path, {})

    assert load_stage1_routing(path) == {}


def test_load_stage1_routing_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(Stage1RoutingValidationError, match="not found"):
        load_stage1_routing(tmp_path / "missing.json")


def test_load_stage1_routing_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "stage1_routing.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(Stage1RoutingValidationError, match=r"invalid stage1_routing\.json"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_object_root(tmp_path: Path) -> None:
    path = _write(tmp_path, [])

    with pytest.raises(Stage1RoutingValidationError, match="must be an object"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_unsupported_category(tmp_path: Path) -> None:
    path = _write(tmp_path, {"usa": _VALID_JRA_CONFIG})

    with pytest.raises(Stage1RoutingValidationError, match="unsupported categories"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_object_category_entry(tmp_path: Path) -> None:
    path = _write(tmp_path, {"jra": []})

    with pytest.raises(Stage1RoutingValidationError, match="must be an object"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_missing_key(tmp_path: Path) -> None:
    incomplete = dict(_VALID_JRA_CONFIG)
    del incomplete["stddev_threshold"]
    path = _write(tmp_path, {"jra": incomplete})

    with pytest.raises(Stage1RoutingValidationError, match="keys differ"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_unexpected_key(tmp_path: Path) -> None:
    extra = dict(_VALID_JRA_CONFIG)
    extra["extra_field"] = "nope"
    path = _write(tmp_path, {"jra": extra})

    with pytest.raises(Stage1RoutingValidationError, match="keys differ"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_boolean_enabled(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["enabled"] = "yes"
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.enabled must be a boolean"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_empty_model_version(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["model_version"] = ""
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.model_version must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_string_model_version(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["model_version"] = 42
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.model_version must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_zero_feature_count(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["feature_count"] = 0
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.feature_count must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_bool_feature_count(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["feature_count"] = True
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.feature_count must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_int_feature_count(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["feature_count"] = 235.5
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.feature_count must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_unsupported_architecture(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["architecture"] = "tensorflow"
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match="unsupported value"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_empty_architecture(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["architecture"] = ""
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.architecture must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_zero_stddev_threshold(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["stddev_threshold"] = 0
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.stddev_threshold must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_negative_stddev_threshold(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["stddev_threshold"] = -0.1
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.stddev_threshold must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_bool_stddev_threshold(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["stddev_threshold"] = True
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.stddev_threshold must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_rejects_non_numeric_stddev_threshold(tmp_path: Path) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["stddev_threshold"] = "0.3"
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(Stage1RoutingValidationError, match=r"\.stddev_threshold must be"):
        load_stage1_routing(path)


def test_load_stage1_routing_accepts_int_stddev_threshold(tmp_path: Path) -> None:
    """An integral threshold (e.g. 1) is a valid float value, not rejected."""
    ok = dict(_VALID_JRA_CONFIG)
    ok["stddev_threshold"] = 1
    path = _write(tmp_path, {"jra": ok})

    routing = load_stage1_routing(path)

    assert routing["jra"].stddev_threshold == 1.0


def test_load_stage1_routing_rejects_non_boolean_enable_stddev_safety_net(
    tmp_path: Path,
) -> None:
    bad = dict(_VALID_JRA_CONFIG)
    bad["enable_stddev_safety_net"] = "false"
    path = _write(tmp_path, {"jra": bad})

    with pytest.raises(
        Stage1RoutingValidationError, match=r"\.enable_stddev_safety_net must be a boolean"
    ):
        load_stage1_routing(path)


def test_load_stage1_routing_accepts_stddev_safety_net_disabled(tmp_path: Path) -> None:
    """A category whose fused Stage-2 score cannot exhibit the collapse
    signature (e.g. a within-race z-normalized blend) opts out of the second
    gate condition explicitly, rather than via a sentinel threshold value."""
    disabled_net = dict(_VALID_JRA_CONFIG)
    disabled_net["enable_stddev_safety_net"] = False
    path = _write(tmp_path, {"jra": disabled_net})

    routing = load_stage1_routing(path)

    assert routing["jra"].enable_stddev_safety_net is False


def test_load_stage1_routing_accepts_disabled_category(tmp_path: Path) -> None:
    disabled = dict(_VALID_JRA_CONFIG)
    disabled["enabled"] = False
    path = _write(tmp_path, {"jra": disabled})

    routing = load_stage1_routing(path)

    assert routing["jra"].enabled is False


# ---------------------------------------------------------------------------
# race_has_fresh_odds
# ---------------------------------------------------------------------------


def test_race_has_fresh_odds_true_when_every_entry_has_ninkijun() -> None:
    entries = [{"tansho_ninkijun": 1}, {"tansho_ninkijun": 2}]

    assert race_has_fresh_odds(entries) is True


def test_race_has_fresh_odds_true_when_only_some_entries_have_ninkijun() -> None:
    """A single late-scratch-style gap is NOT the whole-race outage condition."""
    entries = [{"tansho_ninkijun": 1}, {"tansho_ninkijun": None}]

    assert race_has_fresh_odds(entries) is True


def test_race_has_fresh_odds_false_when_every_entry_lacks_ninkijun() -> None:
    entries = [{"tansho_ninkijun": None}, {"tansho_ninkijun": None}]

    assert race_has_fresh_odds(entries) is False


def test_race_has_fresh_odds_false_when_field_absent_entirely() -> None:
    entries = [{"umaban": 1}, {"umaban": 2}]

    assert race_has_fresh_odds(entries) is False


def test_race_has_fresh_odds_false_for_empty_entries() -> None:
    assert race_has_fresh_odds([]) is False


def test_race_has_fresh_odds_coerces_string_ninkijun() -> None:
    entries = [{"tansho_ninkijun": "3"}]

    assert race_has_fresh_odds(entries) is True


def test_race_has_fresh_odds_treats_blank_string_as_missing() -> None:
    entries = [{"tansho_ninkijun": ""}, {"tansho_ninkijun": None}]

    assert race_has_fresh_odds(entries) is False


def test_race_has_fresh_odds_treats_00_placeholder_string_as_missing() -> None:
    """Raw JVD odds/popularity columns use a non-NULL ``'00'`` placeholder for
    "unconfirmed" (see reference_jvd_placeholder_semantics) -- not every
    upstream SQL path strips it before casting to int, so this module must
    treat a coerced ``0`` the same as missing, not as a real rank."""
    entries = [{"tansho_ninkijun": "00"}, {"tansho_ninkijun": "00"}]

    assert race_has_fresh_odds(entries) is False


def test_race_has_fresh_odds_treats_int_zero_as_missing() -> None:
    entries = [{"tansho_ninkijun": 0}, {"tansho_ninkijun": 0}]

    assert race_has_fresh_odds(entries) is False


def test_race_has_fresh_odds_true_when_only_some_entries_are_00_placeholder() -> None:
    """A single 0/'00' entry alongside a real rank is NOT the whole-race
    outage condition -- mirrors the existing partial-None precedent."""
    entries = [{"tansho_ninkijun": 1}, {"tansho_ninkijun": "00"}]

    assert race_has_fresh_odds(entries) is True


def test_race_has_fresh_odds_treats_negative_ninkijun_as_missing() -> None:
    """Defensive: a real ninkijun is never negative; treat it the same as a
    placeholder rather than trusting a corrupt/impossible value as fresh."""
    entries = [{"tansho_ninkijun": -1}]

    assert race_has_fresh_odds(entries) is False


def test_preserved_odds_gate_is_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, raising=False)

    assert preserved_odds_gate_enabled() is False
    assert race_has_fresh_odds([{"tansho_odds": 3.5}]) is False


def test_preserved_odds_gate_rejects_non_one_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "true")

    assert preserved_odds_gate_enabled() is False


def test_preserved_odds_gate_accepts_valid_full_board_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, " 1 ")
    entries = [
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": "3.5"},
        {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 8.2},
    ]

    assert preserved_odds_gate_enabled() is True
    assert race_has_fresh_odds(entries) is True


def test_preserved_odds_gate_rejects_missing_or_nonpositive_odds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"tansho_ninkijun": None, "tansho_odds": None},
        {"tansho_ninkijun": None, "tansho_odds": 0},
        {"tansho_ninkijun": None, "tansho_odds": -1},
    ]

    assert race_has_fresh_odds(entries) is False


def test_preserved_odds_gate_accepts_complete_canonical_rank_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"tansho_ninkijun": 2, "tansho_odds": 4.8},
        {"tansho_ninkijun": 1, "tansho_odds": 2.1},
    ]

    assert race_has_fresh_odds(entries) is True


def test_preserved_odds_gate_rejects_singleton_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")

    assert race_has_fresh_odds(
        [{"popularity_score": 0.0, "tansho_ninkijun": 1, "tansho_odds": 2.0}]
    ) is False


def test_preserved_odds_gate_rejects_partial_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 1.0},
        {"popularity_score": 0.5, "tansho_ninkijun": None, "tansho_odds": None},
    ]

    assert race_has_fresh_odds(entries) is False


def test_preserved_odds_gate_rejects_duplicate_or_missing_ranks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 2.0},
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 3.0},
    ]

    assert race_has_fresh_odds(entries) is False


def test_preserved_odds_gate_rejects_nonpositive_odds_with_complete_ranks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"tansho_ninkijun": 1, "tansho_odds": 2.0},
        {"tansho_ninkijun": 2, "tansho_odds": 0},
    ]

    assert race_has_fresh_odds(entries) is False


def test_preserved_odds_gate_rejects_odds_rank_order_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 9.0},
        {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 2.0},
    ]

    assert race_has_fresh_odds(entries) is False


@pytest.mark.parametrize(
    ("mode", "entries", "expected"),
    [
        (
            "full",
            [
                {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 2.0},
                {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 5.0},
            ],
            True,
        ),
        (
            "rescore",
            [
                {"popularity_score": 0.5, "tansho_ninkijun": 1, "tansho_odds": 2.0},
                {"popularity_score": 0.5, "tansho_ninkijun": 2, "tansho_odds": 5.0},
            ],
            True,
        ),
        (
            "partial",
            [
                {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 1.0},
                {"popularity_score": 0.5, "tansho_ninkijun": None, "tansho_odds": None},
            ],
            False,
        ),
        (
            "scratch-excluded",
            [
                {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 2.0},
                {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 5.0},
            ],
            True,
        ),
        (
            "missing",
            [
                {"popularity_score": 0.5, "tansho_ninkijun": None, "tansho_odds": None},
                {"popularity_score": 0.5, "tansho_ninkijun": None, "tansho_odds": None},
            ],
            False,
        ),
    ],
)
def test_preserved_odds_gate_mode_parity(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    entries: list[dict[str, object]],
    expected: bool,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")

    assert race_has_fresh_odds(entries) is expected, mode


def test_preserved_odds_gate_rejects_non_integral_derived_rank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 2.0},
        {"popularity_score": 0.4, "tansho_ninkijun": None, "tansho_odds": 3.0},
        {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 4.0},
    ]

    assert race_has_fresh_odds(entries) is False


# ---------------------------------------------------------------------------
# compute_predicted_score_stddev / is_score_spread_degraded
# ---------------------------------------------------------------------------


def test_compute_predicted_score_stddev_healthy_spread() -> None:
    scores = [3.0, 1.0, 2.0, 0.5, -1.0]

    stddev = compute_predicted_score_stddev(scores)

    assert stddev == pytest.approx(1.35647, rel=1e-4)


def test_compute_predicted_score_stddev_collapsed_spread() -> None:
    scores = [0.501, 0.499, 0.500, 0.502]

    stddev = compute_predicted_score_stddev(scores)

    assert stddev < 0.01


def test_compute_predicted_score_stddev_empty_returns_zero() -> None:
    assert compute_predicted_score_stddev([]) == 0.0


def test_compute_predicted_score_stddev_single_value_returns_zero() -> None:
    assert compute_predicted_score_stddev([1.23]) == 0.0


def test_compute_predicted_score_stddev_identical_values_returns_zero() -> None:
    assert compute_predicted_score_stddev([0.5, 0.5, 0.5]) == 0.0


def test_is_score_spread_degraded_below_threshold() -> None:
    assert is_score_spread_degraded(0.1, 0.3) is True


def test_is_score_spread_degraded_at_threshold_is_not_degraded() -> None:
    assert is_score_spread_degraded(0.3, 0.3) is False


def test_is_score_spread_degraded_above_threshold() -> None:
    assert is_score_spread_degraded(1.2, 0.3) is False


# ---------------------------------------------------------------------------
# extract_predicted_scores
# ---------------------------------------------------------------------------


def _row_with_score(score: object) -> list[object]:
    """Build a full-width ``INSERT_COLUMNS``-shaped row with one score cell."""
    row: list[object] = [None] * len(INSERT_COLUMNS)
    row[PREDICTED_SCORE_COLUMN_INDEX] = score
    return row


def test_extract_predicted_scores_reads_predicted_score_column() -> None:
    rows = [_row_with_score(1.5), _row_with_score(-0.75)]

    assert extract_predicted_scores(rows) == [1.5, -0.75]


def test_extract_predicted_scores_coerces_int_cell() -> None:
    rows = [_row_with_score(2)]

    assert extract_predicted_scores(rows) == [2.0]


def test_extract_predicted_scores_empty_rows() -> None:
    assert extract_predicted_scores([]) == []


def test_extract_predicted_scores_rejects_bool_cell() -> None:
    rows = [_row_with_score(True)]

    with pytest.raises(TypeError, match="must not be a bool"):
        extract_predicted_scores(rows)


def test_extract_predicted_scores_rejects_non_numeric_cell() -> None:
    rows = [_row_with_score("oops")]

    with pytest.raises(TypeError, match="must be numeric"):
        extract_predicted_scores(rows)


# ---------------------------------------------------------------------------
# resolve_stage1_gate (integration of all of the above)
# ---------------------------------------------------------------------------

_CONFIG = Stage1CategoryConfig(
    enabled=True,
    model_version="jra-cb-stage1-marketfree235-2013",
    feature_count=235,
    architecture="catboost",
    stddev_threshold=0.3,
    enable_stddev_safety_net=True,
)

_FRESH_ENTRIES = [{"tansho_ninkijun": 1}, {"tansho_ninkijun": 2}, {"tansho_ninkijun": 3}]
_STALE_ENTRIES = [{"tansho_ninkijun": None}, {"tansho_ninkijun": None}]
_HEALTHY_SCORES = [1.2, 0.4, -0.3, 0.9]
_DEGENERATE_SCORES = [0.501, 0.500, 0.499]


def test_resolve_stage1_gate_disabled_when_config_none() -> None:
    decision = resolve_stage1_gate(config=None, entries=_STALE_ENTRIES, stage2_scores=[])

    assert decision == Stage1GateDecision(use_stage1=False, reason="disabled", stddev=None)


def test_resolve_stage1_gate_disabled_when_config_enabled_false() -> None:
    disabled_config = Stage1CategoryConfig(
        enabled=False,
        model_version="jra-cb-stage1-marketfree235-2013",
        feature_count=235,
        architecture="catboost",
        stddev_threshold=0.3,
        enable_stddev_safety_net=True,
    )

    decision = resolve_stage1_gate(config=disabled_config, entries=_STALE_ENTRIES, stage2_scores=[])

    assert decision == Stage1GateDecision(use_stage1=False, reason="disabled", stddev=None)


def test_resolve_stage1_gate_passes_through_on_fresh_odds_and_healthy_spread() -> None:
    decision = resolve_stage1_gate(
        config=_CONFIG, entries=_FRESH_ENTRIES, stage2_scores=_HEALTHY_SCORES
    )

    assert decision.use_stage1 is False
    assert decision.reason == "fresh"
    assert decision.stddev is not None
    assert decision.stddev > _CONFIG.stddev_threshold


def test_resolve_stage1_gate_trips_on_odds_missing_regardless_of_scores() -> None:
    decision = resolve_stage1_gate(
        config=_CONFIG, entries=_STALE_ENTRIES, stage2_scores=_HEALTHY_SCORES
    )

    assert decision == Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)


def test_resolve_stage1_gate_trips_on_odds_missing_even_with_no_scores() -> None:
    """Freshness is checked before scores are consulted at all -- an empty
    ``stage2_scores`` (e.g. a degenerate feature matrix upstream) must not
    prevent an odds-missing race from routing to Stage-1."""
    decision = resolve_stage1_gate(config=_CONFIG, entries=_STALE_ENTRIES, stage2_scores=[])

    assert decision == Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)


def test_resolve_stage1_gate_uses_preserved_odds_only_when_flag_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entries = [
        {"popularity_score": 0.0, "tansho_ninkijun": None, "tansho_odds": 2.4},
        {"popularity_score": 1.0, "tansho_ninkijun": None, "tansho_odds": 5.1},
    ]

    monkeypatch.delenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, raising=False)
    current = resolve_stage1_gate(config=_CONFIG, entries=entries, stage2_scores=_HEALTHY_SCORES)
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    corrected = resolve_stage1_gate(config=_CONFIG, entries=entries, stage2_scores=_HEALTHY_SCORES)

    assert current == Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)
    assert corrected.use_stage1 is False
    assert corrected.reason == "fresh"
    assert corrected.stddev is not None


def test_resolve_stage1_gate_flag_keeps_missing_board_on_stage1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STAGE1_PRESERVED_ODDS_GATE_ENABLED_ENV, "1")
    entries = [
        {"tansho_ninkijun": None, "tansho_odds": None},
        {"tansho_ninkijun": None, "tansho_odds": None},
    ]

    decision = resolve_stage1_gate(config=_CONFIG, entries=entries, stage2_scores=_HEALTHY_SCORES)

    assert decision == Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)


def test_resolve_stage1_gate_trips_on_degraded_spread_even_when_odds_fresh() -> None:
    """The doc's second insurance layer: odds LOOK fresh but the champion's
    own ranking still collapsed -- the stddev safety net must still catch it."""
    decision = resolve_stage1_gate(
        config=_CONFIG, entries=_FRESH_ENTRIES, stage2_scores=_DEGENERATE_SCORES
    )

    assert decision.use_stage1 is True
    assert decision.reason == "score-spread-degraded"
    assert decision.stddev is not None
    assert decision.stddev < _CONFIG.stddev_threshold


def test_resolve_stage1_gate_at_exact_threshold_does_not_trip() -> None:
    scores = [0.15, -0.15]  # pstdev == 0.15, below the doc's 0.3 default
    boundary_config = Stage1CategoryConfig(
        enabled=True,
        model_version="jra-cb-stage1-marketfree235-2013",
        feature_count=235,
        architecture="catboost",
        stddev_threshold=compute_predicted_score_stddev(scores),
        enable_stddev_safety_net=True,
    )

    decision = resolve_stage1_gate(
        config=boundary_config, entries=_FRESH_ENTRIES, stage2_scores=scores
    )

    assert decision.use_stage1 is False
    assert decision.reason == "fresh"


# ---------------------------------------------------------------------------
# resolve_stage1_gate: enable_stddev_safety_net opt-out (a category whose
# Stage-2 score is within-race normalized, e.g. NAR's z-fusion blend, cannot
# use the stddev signature at all -- see the module docstring)
# ---------------------------------------------------------------------------

_CONFIG_NET_DISABLED = Stage1CategoryConfig(
    enabled=True,
    model_version="nar-cb-stage1-marketfree-example",
    feature_count=180,
    architecture="xgboost",
    stddev_threshold=0.3,
    enable_stddev_safety_net=False,
)


def test_resolve_stage1_gate_net_disabled_never_trips_on_degenerate_scores() -> None:
    """Even a maximally collapsed score spread must not trip Stage-1 when the
    category has explicitly opted out of the stddev safety net."""
    decision = resolve_stage1_gate(
        config=_CONFIG_NET_DISABLED, entries=_FRESH_ENTRIES, stage2_scores=_DEGENERATE_SCORES
    )

    assert decision == Stage1GateDecision(use_stage1=False, reason="fresh", stddev=None)


def test_resolve_stage1_gate_net_disabled_never_trips_on_identical_scores() -> None:
    """Zero variance (the most extreme possible collapse) still must not trip
    when the net is disabled."""
    decision = resolve_stage1_gate(
        config=_CONFIG_NET_DISABLED, entries=_FRESH_ENTRIES, stage2_scores=[0.5, 0.5, 0.5]
    )

    assert decision.use_stage1 is False
    assert decision.reason == "fresh"
    assert decision.stddev is None


def test_resolve_stage1_gate_net_disabled_stddev_is_never_computed() -> None:
    """compute_predicted_score_stddev is skipped entirely (not merely
    ignored) -- the reported stddev is None, not a computed-but-unused value."""
    decision = resolve_stage1_gate(
        config=_CONFIG_NET_DISABLED, entries=_FRESH_ENTRIES, stage2_scores=_HEALTHY_SCORES
    )

    assert decision.stddev is None


def test_resolve_stage1_gate_net_disabled_freshness_gate_still_trips() -> None:
    """Disabling the stddev safety net must not weaken the (independent)
    freshness gate."""
    decision = resolve_stage1_gate(
        config=_CONFIG_NET_DISABLED, entries=_STALE_ENTRIES, stage2_scores=_HEALTHY_SCORES
    )

    assert decision == Stage1GateDecision(use_stage1=True, reason="odds-missing", stddev=None)
