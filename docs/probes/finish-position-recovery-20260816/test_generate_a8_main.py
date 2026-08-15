from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROBE_DIR = Path(__file__).resolve().parent
if str(PROBE_DIR) not in sys.path:
    sys.path.insert(0, str(PROBE_DIR))

from generate_a8_main import (
    BoardEntry,
    BoardInvalidError,
    GenerationError,
    check_board,
    expected_competition_rank,
    load_board_json,
    overlay_market_features,
    parse_args,
    parse_official_card_html,
    production_market_features,
    run,
)

REPO = Path(__file__).resolve().parents[3]
BOARD_JSON = REPO / "tmp/jacques-le-marois-a8-market-20260816/hot-board-check-0712.json"
OFFICIAL_HTML = (
    REPO / "tmp/jacques-le-marois-a8-market-20260816/jra-jacques-le-marois-20260816-0712.utf8.html"
)
MARKET_NULL = (
    REPO / "tmp/jacques-le-marois-a8-experimental-20260815/a8-market-null-prediction-full.json"
)


def test_check_board_accepts_0712_competition_ranks() -> None:
    entries = load_board_json(BOARD_JSON)
    check = check_board(entries)
    assert check.valid is True
    assert check.positive_finite_odds is True
    assert check.competition_ranking is True
    assert check.odds_nondecreasing is True
    assert check.same_rank_same_odds is True
    assert check.sorted_ranks == (1, 2, 3, 4, 4, 6, 7, 8, 9, 10)


def test_check_board_rejects_missing_runner() -> None:
    entries = load_board_json(BOARD_JSON)[:9]
    check = check_board(entries)
    assert check.valid is False
    assert check.fail_reasons[0] == "runner_count=9"
    assert check.fail_reasons[1] == "odds_not_10_of_10_positive_finite"


def test_check_board_rejects_non_positive_odds() -> None:
    entries = load_board_json(BOARD_JSON)
    broken = [
        BoardEntry(umaban=1, name="x", odds=0.0, rank=1),
        entries[1],
        entries[2],
        entries[3],
        entries[4],
        entries[5],
        entries[6],
        entries[7],
        entries[8],
        entries[9],
    ]
    check = check_board(broken)
    assert check.valid is False
    assert "odds_not_10_of_10_positive_finite" in check.fail_reasons


def test_check_board_rejects_rank_that_is_not_competition() -> None:
    entries = load_board_json(BOARD_JSON)
    broken = [
        BoardEntry(umaban=entries[0].umaban, name=entries[0].name, odds=entries[0].odds, rank=1),
        entries[1],
        entries[2],
        entries[3],
        entries[4],
        entries[5],
        entries[6],
        entries[7],
        entries[8],
        entries[9],
    ]
    check = check_board(broken)
    assert check.valid is False
    assert check.competition_ranking is False


def test_check_board_rejects_same_rank_different_odds() -> None:
    entries = [
        BoardEntry(umaban=1, name="a", odds=2.0, rank=1),
        BoardEntry(umaban=2, name="b", odds=3.0, rank=1),
        BoardEntry(umaban=3, name="c", odds=4.0, rank=3),
        BoardEntry(umaban=4, name="d", odds=5.0, rank=4),
        BoardEntry(umaban=5, name="e", odds=6.0, rank=5),
        BoardEntry(umaban=6, name="f", odds=7.0, rank=6),
        BoardEntry(umaban=7, name="g", odds=8.0, rank=7),
        BoardEntry(umaban=8, name="h", odds=9.0, rank=8),
        BoardEntry(umaban=9, name="i", odds=10.0, rank=9),
        BoardEntry(umaban=10, name="j", odds=11.0, rank=10),
    ]
    check = check_board(entries)
    assert check.valid is False
    assert "same_rank_different_odds" in check.fail_reasons


def test_check_board_rejects_decreasing_odds() -> None:
    entries = [
        BoardEntry(umaban=1, name="a", odds=3.0, rank=1),
        BoardEntry(umaban=2, name="b", odds=2.0, rank=2),
        BoardEntry(umaban=3, name="c", odds=4.0, rank=3),
        BoardEntry(umaban=4, name="d", odds=5.0, rank=4),
        BoardEntry(umaban=5, name="e", odds=6.0, rank=5),
        BoardEntry(umaban=6, name="f", odds=7.0, rank=6),
        BoardEntry(umaban=7, name="g", odds=8.0, rank=7),
        BoardEntry(umaban=8, name="h", odds=9.0, rank=8),
        BoardEntry(umaban=9, name="i", odds=10.0, rank=9),
        BoardEntry(umaban=10, name="j", odds=11.0, rank=10),
    ]
    check = check_board(entries)
    assert check.valid is False
    assert "odds_not_nondecreasing_with_rank" in check.fail_reasons


def test_expected_competition_rank_skips_after_tie() -> None:
    entries = [
        BoardEntry(umaban=1, name="a", odds=2.0, rank=1),
        BoardEntry(umaban=2, name="b", odds=3.0, rank=2),
        BoardEntry(umaban=3, name="c", odds=3.0, rank=2),
        BoardEntry(umaban=4, name="d", odds=5.0, rank=4),
    ]
    assert expected_competition_rank(entries[0], entries) == 1
    assert expected_competition_rank(entries[1], entries) == 2
    assert expected_competition_rank(entries[2], entries) == 2
    assert expected_competition_rank(entries[3], entries) == 4


def test_html_parser_reads_blinker_and_red_odds() -> None:
    html = OFFICIAL_HTML.read_text(encoding="utf-8")
    entries = parse_official_card_html(html)
    assert [entry.umaban for entry in entries] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert entries[2] == BoardEntry(umaban=3, name="シックスペンス", odds=3.4, rank=1)
    assert entries[3] == BoardEntry(umaban=4, name="シュトラウス", odds=6.9, rank=4)
    assert entries[4] == BoardEntry(umaban=5, name="モアサンダー", odds=6.9, rank=4)


def test_production_market_features_match_0712_board() -> None:
    entries = load_board_json(BOARD_JSON)
    features = production_market_features(entries)
    sixpence = features[3]
    assert sixpence["tansho_odds_raw"] == 3.4
    assert sixpence["tansho_ninkijun_raw"] == 1.0
    assert sixpence["popularity_score"] == 0.0
    assert sixpence["inverse_odds_implied_prob"] == pytest.approx(1.0 / 3.4)
    assert sixpence["inverse_odds_rank_in_race"] == 1.0
    assert sixpence["popularity_rank_in_race"] == 1.0
    assert sixpence["horse_popularity_vs_field"] == pytest.approx(0.1)
    assert sixpence["field_dominant_favorite_indicator"] == pytest.approx(3.4 / 4.1)
    assert sixpence["sim_odds_rank_correlation"] is None
    strauss = features[4]
    more_thunder = features[5]
    assert strauss["tansho_ninkijun_raw"] == 4.0
    assert more_thunder["tansho_ninkijun_raw"] == 4.0
    assert strauss["popularity_score"] == pytest.approx(1.0 / 3.0)
    assert strauss["odds_score"] == more_thunder["odds_score"]
    assert features[6]["horse_popularity_vs_field"] == 1.0


def test_production_market_features_rejects_single_runner() -> None:
    with pytest.raises(BoardInvalidError, match="at least two runners"):
        production_market_features([BoardEntry(umaban=1, name="a", odds=2.0, rank=1)])


def test_overlay_injects_board_features_and_keeps_similar_null() -> None:
    market_null = json.loads(MARKET_NULL.read_text(encoding="utf-8"))
    features = production_market_features(load_board_json(BOARD_JSON))
    overlaid = overlay_market_features(market_null, features)
    runners = overlaid["runners"]
    assert isinstance(runners, list)
    first = runners[0]
    assert isinstance(first, dict)
    values = first["featureValues"]
    assert isinstance(values, dict)
    assert values["tansho_odds_raw"] == 16.4
    assert values["sim_fav_win_rate"] is None
    reasons = first["nullReasons"]
    assert isinstance(reasons, dict)
    assert reasons["sim_fav_win_rate"] == "deauville_not_in_jra_similar_race_pool"
    assert first["nonnullCount"] == 47


def test_overlay_rejects_missing_umaban() -> None:
    market_null = json.loads(MARKET_NULL.read_text(encoding="utf-8"))
    with pytest.raises(GenerationError, match="missing umaban"):
        overlay_market_features(market_null, {})


def test_load_board_json_rejects_missing_runners(tmp_path: Path) -> None:
    path = tmp_path / "board.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(GenerationError, match="no runners array"):
        load_board_json(path)


def test_dry_run_writes_isolated_scores(tmp_path: Path) -> None:
    args = parse_args(
        [
            "--dry-run",
            "--board-json",
            str(BOARD_JSON),
            "--market-null-json",
            str(MARKET_NULL),
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert run(args) == 0
    report = json.loads((tmp_path / "a8-main-report.json").read_text(encoding="utf-8"))
    assert report["board"]["validFullBoard"] is True
    assert report["writes"]["postgres"] is False
    assert report["writes"]["r2"] is False
    assert report["writes"]["cache"] is False
    predictions = report["predictions"]
    assert isinstance(predictions, list)
    assert len(predictions) == 10
    first = predictions[0]
    assert isinstance(first, dict)
    assert first["predictedRank"] == 1
    assert first["umaban"] == 3
    assert first["horseName"] == "Sixpence"
    summary = json.loads(
        (tmp_path / "a8-market-overlay-prediction-summary.json").read_text(encoding="utf-8")
    )
    assert summary["summary"]["label"] == "DRY-RUN / MARKET-OVERLAY / NO PRODUCTION WRITE"


def test_invalid_board_exits_two_without_scores(tmp_path: Path) -> None:
    broken = tmp_path / "broken.json"
    payload = json.loads(BOARD_JSON.read_text(encoding="utf-8"))
    payload["runners"] = payload["runners"][:8]
    broken.write_text(json.dumps(payload), encoding="utf-8")
    args = parse_args(["--dry-run", "--board-json", str(broken), "--output-dir", str(tmp_path)])
    assert run(args) == 2
    assert (tmp_path / "a8-board-invalid.json").is_file()
    assert not (tmp_path / "a8-market-overlay-prediction-full.json").exists()
