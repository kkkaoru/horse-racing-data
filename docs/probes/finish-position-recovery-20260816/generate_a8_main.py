#!/usr/bin/env python3
"""Isolated A8 Jacques le Marois main-generation command.

Judges the official JRA board with competition ranking, injects the 13
board-derived production market features, leaves the 3 similar-race market
features NULL, and rescores the existing market-null experimental vectors.
Never writes PostgreSQL, R2, or viewer caches.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, TextIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
DEFAULT_BOARD_JSON: Final[Path] = (
    REPO_ROOT / "tmp/jacques-le-marois-a8-market-20260816/hot-board-check-0712.json"
)
DEFAULT_MARKET_NULL_JSON: Final[Path] = (
    REPO_ROOT / "tmp/jacques-le-marois-a8-experimental-20260815/a8-market-null-prediction-full.json"
)
DEFAULT_MODEL_DIR: Final[Path] = (
    REPO_ROOT
    / "apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/artifacts/jra-cb-v9-sim-2013-CLEAN"
)
DEFAULT_DRY_RUN_DIR: Final[Path] = REPO_ROOT / "tmp/jacques-le-marois-a8-dry-run-20260816"
DEFAULT_EXECUTE_DIR: Final[Path] = REPO_ROOT / "tmp/jacques-le-marois-a8-main-20260816"
OFFICIAL_CARD_URL: Final[str] = (
    "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73"
)
USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 oversea-horse-race/a8-main-generation"
)
JST: Final[ZoneInfo] = ZoneInfo("Asia/Tokyo")
EXPECTED_RUNNER_COUNT: Final[int] = 10
FEATURE_COUNT: Final[int] = 250
UNIFORM_SOFTMAX_SHARE: Final[float] = 1.0 / EXPECTED_RUNNER_COUNT
NEARLY_UNIFORM_SHARE_RANGE: Final[float] = 0.02
ODDS_LN_CAP: Final[float] = 300.0
FETCH_TIMEOUT_SECONDS: Final[int] = 20

MARKET16: Final[tuple[str, ...]] = (
    "popularity_score",
    "odds_score",
    "tansho_odds_raw",
    "tansho_ninkijun_raw",
    "inverse_odds_implied_prob",
    "inverse_odds_market_share",
    "inverse_odds_rank_in_race",
    "popularity_rank_in_race",
    "odds_score_diff_from_race_avg",
    "popularity_score_diff_from_race_avg",
    "popularity_odds_disagreement",
    "field_dominant_favorite_indicator",
    "horse_popularity_vs_field",
    "sim_odds_rank_correlation",
    "sim_odds_correlation_variance",
    "sim_fav_win_rate",
)
BOARD_DERIVED_MARKET: Final[frozenset[str]] = frozenset(
    {
        "popularity_score",
        "odds_score",
        "tansho_odds_raw",
        "tansho_ninkijun_raw",
        "inverse_odds_implied_prob",
        "inverse_odds_market_share",
        "inverse_odds_rank_in_race",
        "popularity_rank_in_race",
        "odds_score_diff_from_race_avg",
        "popularity_score_diff_from_race_avg",
        "popularity_odds_disagreement",
        "field_dominant_favorite_indicator",
        "horse_popularity_vs_field",
    }
)
SIMILAR_RACE_MARKET: Final[frozenset[str]] = frozenset(
    {
        "sim_odds_rank_correlation",
        "sim_odds_correlation_variance",
        "sim_fav_win_rate",
    }
)

ROW_PATTERN: Final[re.Pattern[str]] = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
UMABAN_PATTERN: Final[re.Pattern[str]] = re.compile(r'<td class="num">\s*(\d+)', re.IGNORECASE)
NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'<div class="txt">(?:<a\b[^>]*>)?([^<]+)',
    re.IGNORECASE,
)
ODDS_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"<strong(?:\s+class=\"[^\"]*\")?>([0-9]+(?:\.[0-9]+)?)</strong>",
    re.IGNORECASE,
)
RANK_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'<span class="pop_rank">\((\d+)<span>番人気</span>\)</span>',
    re.IGNORECASE,
)


class BoardInvalidError(ValueError):
    """Raised when the official board fails the revised valid-full checks."""


class GenerationError(RuntimeError):
    """Raised when isolated generation cannot complete."""


@dataclass(frozen=True, slots=True)
class BoardEntry:
    umaban: int
    name: str
    odds: float
    rank: int


@dataclass(frozen=True, slots=True)
class BoardCheck:
    valid: bool
    runner_count: int
    positive_finite_odds: bool
    competition_ranking: bool
    odds_nondecreasing: bool
    same_rank_same_odds: bool
    fail_reasons: tuple[str, ...]
    sorted_ranks: tuple[int, ...]


def now_jst() -> datetime:
    return datetime.now(tz=JST)


def parse_official_card_html(html: str) -> list[BoardEntry]:
    entries: list[BoardEntry] = []
    seen: set[int] = set()
    for row in ROW_PATTERN.findall(html):
        umaban_match = UMABAN_PATTERN.search(row)
        name_match = NAME_PATTERN.search(row)
        odds_match = ODDS_PATTERN.search(row)
        rank_match = RANK_PATTERN.search(row)
        if umaban_match is None or name_match is None or odds_match is None or rank_match is None:
            continue
        umaban = int(umaban_match.group(1))
        if umaban in seen:
            continue
        seen.add(umaban)
        entries.append(
            BoardEntry(
                umaban=umaban,
                name=name_match.group(1).strip(),
                odds=float(odds_match.group(1)),
                rank=int(rank_match.group(1)),
            )
        )
    return sorted(entries, key=lambda entry: entry.umaban)


def load_board_json(path: Path) -> list[BoardEntry]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_runners = payload.get("runners")
    if not isinstance(raw_runners, list):
        raise GenerationError(f"{path} has no runners array")
    entries: list[BoardEntry] = []
    for item in raw_runners:
        if not isinstance(item, dict):
            raise GenerationError(f"{path} runner is not an object")
        umaban = item.get("umaban")
        name = item.get("name")
        odds = item.get("odds")
        rank = item.get("ninki")
        if not isinstance(umaban, int) or isinstance(umaban, bool):
            raise GenerationError(f"{path} runner umaban is not an int")
        if not isinstance(name, str) or name == "":
            raise GenerationError(f"{path} runner name is missing")
        if not isinstance(odds, (int, float)) or isinstance(odds, bool):
            raise GenerationError(f"{path} runner odds is not a number")
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise GenerationError(f"{path} runner ninki is not an int")
        entries.append(BoardEntry(umaban=umaban, name=name, odds=float(odds), rank=rank))
    return sorted(entries, key=lambda entry: entry.umaban)


def expected_competition_rank(entry: BoardEntry, entries: Sequence[BoardEntry]) -> int:
    return 1 + sum(1 for other in entries if other.odds < entry.odds)


def check_board(entries: Sequence[BoardEntry]) -> BoardCheck:
    reasons: list[str] = []
    runner_count = len(entries)
    umabans = [entry.umaban for entry in entries]
    positive_finite = runner_count == EXPECTED_RUNNER_COUNT and all(
        math.isfinite(entry.odds) and entry.odds > 0 for entry in entries
    )
    if runner_count != EXPECTED_RUNNER_COUNT:
        reasons.append(f"runner_count={runner_count}")
    if len(set(umabans)) != runner_count:
        reasons.append("duplicate_umaban")
    if not positive_finite:
        reasons.append("odds_not_10_of_10_positive_finite")

    competition = all(
        entry.rank == expected_competition_rank(entry, entries) for entry in entries
    )
    if not competition:
        reasons.append(
            "competition_ranking_fail ranks="
            + ",".join(str(entry.rank) for entry in entries)
        )

    by_rank = sorted(entries, key=lambda entry: (entry.rank, entry.umaban))
    nondecreasing = True
    same_rank_same_odds = True
    for current, following in zip(by_rank, by_rank[1:], strict=False):
        if current.rank == following.rank:
            if current.odds != following.odds:
                same_rank_same_odds = False
        elif current.odds > following.odds:
            nondecreasing = False
    if not nondecreasing:
        reasons.append("odds_not_nondecreasing_with_rank")
    if not same_rank_same_odds:
        reasons.append("same_rank_different_odds")

    sorted_ranks = tuple(sorted(entry.rank for entry in entries))
    valid = (
        positive_finite
        and competition
        and nondecreasing
        and same_rank_same_odds
        and len(set(umabans)) == runner_count
    )
    return BoardCheck(
        valid=valid,
        runner_count=runner_count,
        positive_finite_odds=positive_finite,
        competition_ranking=competition,
        odds_nondecreasing=nondecreasing,
        same_rank_same_odds=same_rank_same_odds,
        fail_reasons=tuple(reasons),
        sorted_ranks=sorted_ranks,
    )


def _clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, value))


def _sql_rank(values: Sequence[float], *, descending: bool) -> list[int]:
    ranks: list[int] = []
    for value in values:
        if descending:
            better = sum(1 for other in values if other > value)
        else:
            better = sum(1 for other in values if other < value)
        ranks.append(better + 1)
    return ranks


def production_market_features(entries: Sequence[BoardEntry]) -> dict[int, dict[str, float | None]]:
    if len(entries) < 2:
        raise BoardInvalidError("need at least two runners for production market features")
    runner_count = len(entries)
    odds_values = [entry.odds for entry in entries]
    ranks = [entry.rank for entry in entries]
    implied = [1.0 / entry.odds for entry in entries]
    implied_sum = sum(implied)
    if implied_sum <= 0:
        raise BoardInvalidError("implied-probability mass is not positive")
    popularity_scores = [(rank - 1) / (runner_count - 1) for rank in ranks]
    odds_scores = [
        _clamp_unit(math.log(max(odds, 1.0)) / math.log(ODDS_LN_CAP)) for odds in odds_values
    ]
    popularity_avg = sum(popularity_scores) / runner_count
    odds_avg = sum(odds_scores) / runner_count
    inverse_ranks = _sql_rank(implied, descending=True)
    popularity_ranks = _sql_rank([float(rank) for rank in ranks], descending=False)
    favorite_order = sorted(entries, key=lambda entry: (entry.rank, entry.umaban))
    favorite_odds = favorite_order[0].odds
    second_odds = favorite_order[1].odds
    if second_odds == 0:
        raise BoardInvalidError("second-favorite odds is zero")
    field_dominant = favorite_odds / second_odds

    by_umaban: dict[int, dict[str, float | None]] = {}
    for index, entry in enumerate(entries):
        by_umaban[entry.umaban] = {
            "tansho_odds_raw": entry.odds,
            "tansho_ninkijun_raw": float(entry.rank),
            "popularity_score": popularity_scores[index],
            "odds_score": odds_scores[index],
            "inverse_odds_implied_prob": implied[index],
            "inverse_odds_market_share": implied[index] / implied_sum,
            "inverse_odds_rank_in_race": float(inverse_ranks[index]),
            "popularity_rank_in_race": float(popularity_ranks[index]),
            "odds_score_diff_from_race_avg": odds_scores[index] - odds_avg,
            "popularity_score_diff_from_race_avg": popularity_scores[index] - popularity_avg,
            "popularity_odds_disagreement": abs(popularity_scores[index] - odds_scores[index]),
            "field_dominant_favorite_indicator": field_dominant,
            "horse_popularity_vs_field": entry.rank / runner_count,
            "sim_odds_rank_correlation": None,
            "sim_odds_correlation_variance": None,
            "sim_fav_win_rate": None,
        }
    return by_umaban


def overlay_market_features(
    market_null: Mapping[str, object],
    market_by_umaban: Mapping[int, Mapping[str, float | None]],
) -> dict[str, object]:
    runners_raw = market_null.get("runners")
    if not isinstance(runners_raw, list):
        raise GenerationError("market-null JSON has no runners array")
    overlaid: list[dict[str, object]] = []
    for runner in runners_raw:
        if not isinstance(runner, dict):
            raise GenerationError("market-null runner is not an object")
        umaban = runner.get("umaban")
        if not isinstance(umaban, int) or isinstance(umaban, bool):
            raise GenerationError("market-null runner umaban is not an int")
        injected = market_by_umaban.get(umaban)
        if injected is None:
            raise GenerationError(f"board is missing umaban {umaban}")
        feature_values_raw = runner.get("featureValues")
        if not isinstance(feature_values_raw, dict):
            raise GenerationError(f"umaban {umaban} has no featureValues")
        provenance_raw = runner.get("provenance")
        if not isinstance(provenance_raw, dict):
            raise GenerationError(f"umaban {umaban} has no provenance")
        null_reasons_raw = runner.get("nullReasons")
        if not isinstance(null_reasons_raw, dict):
            raise GenerationError(f"umaban {umaban} has no nullReasons")
        feature_values = dict(feature_values_raw)
        provenance = dict(provenance_raw)
        null_reasons = dict(null_reasons_raw)
        for name, value in injected.items():
            feature_values[name] = value
            if value is None:
                provenance[name] = {
                    "source": "not-injected",
                    "reason": "deauville_not_in_jra_similar_race_pool",
                }
                null_reasons[name] = "deauville_not_in_jra_similar_race_pool"
            else:
                provenance[name] = {
                    "source": "jra-official-card-hot-board",
                    "reason": "production formula on official tansho odds/ninki",
                }
                null_reasons.pop(name, None)
        copy = dict(runner)
        copy["featureValues"] = feature_values
        copy["provenance"] = provenance
        copy["nullReasons"] = null_reasons
        copy["nonnullCount"] = sum(cell is not None for cell in feature_values.values())
        copy["nullCount"] = sum(cell is None for cell in feature_values.values())
        overlaid.append(copy)
    return {**dict(market_null), "runners": overlaid}


def score_runners(
    runners: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    model_path: Path,
) -> list[dict[str, object]]:
    import numpy as np
    from catboost import CatBoost

    matrix = np.array(
        [
            [
                0.0 if runner_features(runner)[name] is None else float(runner_features(runner)[name])
                for name in feature_names
            ]
            for runner in runners
        ],
        dtype=float,
    )
    model = CatBoost()
    model.load_model(str(model_path), format="json")
    scores = np.asarray(model.predict(matrix), dtype=float)
    order = np.argsort(-scores, kind="stable")
    ranks = np.empty(len(scores), dtype=int)
    ranks[order] = np.arange(1, len(scores) + 1)
    shifted = np.exp(scores - scores.max())
    softmax = shifted / shifted.sum()
    predictions: list[dict[str, object]] = []
    for index, runner in enumerate(runners):
        umaban = runner.get("umaban")
        horse_name = runner.get("horseName")
        if not isinstance(umaban, int) or isinstance(umaban, bool):
            raise GenerationError("scored runner umaban is not an int")
        if not isinstance(horse_name, str):
            raise GenerationError("scored runner horseName is not a string")
        predictions.append(
            {
                "umaban": umaban,
                "horseName": horse_name,
                "predictedRank": int(ranks[index]),
                "rawScore": float(scores[index]),
                "softmaxTop1Share": float(softmax[index]),
                "calibratedProbability": False,
            }
        )
    return sorted(predictions, key=lambda item: int(item["predictedRank"]))


def runner_features(runner: Mapping[str, object]) -> dict[str, object]:
    values = runner.get("featureValues")
    if not isinstance(values, dict):
        raise GenerationError("runner featureValues is not an object")
    return values


def feature_coverage(runners: Sequence[Mapping[str, object]]) -> dict[str, object]:
    counts: dict[str, int] = {}
    for runner in runners:
        umaban = runner.get("umaban")
        if not isinstance(umaban, int) or isinstance(umaban, bool):
            raise GenerationError("coverage runner umaban is not an int")
        values = runner_features(runner)
        counts[str(umaban)] = sum(cell is not None for cell in values.values())
    if not counts:
        raise GenerationError("coverage has no runners")
    nonnull_values = list(counts.values())
    return {
        "featureCount": FEATURE_COUNT,
        "nonnullByUmaban": counts,
        "nullByUmaban": {
            umaban: FEATURE_COUNT - count for umaban, count in counts.items()
        },
        "nonnullMin": min(nonnull_values),
        "nonnullMax": max(nonnull_values),
        "nonnullLabel": f"{min(nonnull_values)}-{max(nonnull_values)}/{FEATURE_COUNT}",
    }


def softmax_quality(predictions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    shares: list[float] = []
    for prediction in predictions:
        share = prediction.get("softmaxTop1Share")
        if not isinstance(share, (int, float)) or isinstance(share, bool):
            raise GenerationError("softmaxTop1Share is not a number")
        shares.append(float(share))
    if not shares:
        raise GenerationError("predictions have no softmax shares")
    share_min = min(shares)
    share_max = max(shares)
    mean = sum(shares) / len(shares)
    pstdev = math.sqrt(sum((share - mean) ** 2 for share in shares) / len(shares))
    nearly_uniform = (share_max - share_min) < NEARLY_UNIFORM_SHARE_RANGE
    return {
        "softmaxShares": shares,
        "softmaxMin": share_min,
        "softmaxMax": share_max,
        "softmaxPstdev": pstdev,
        "uniformShare": UNIFORM_SOFTMAX_SHARE,
        "nearlyUniform": nearly_uniform,
        "meaning": (
            "softmax nearly uniform; field not separated"
            if nearly_uniform
            else "softmax not uniform; field is separated"
        ),
    }


def fetch_official_card(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    try:
        with urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
            return bytes(response.read())
    except (HTTPError, URLError, TimeoutError) as error:
        raise GenerationError(f"official card fetch failed: {error}") from error


def decode_official_html(raw: bytes) -> str:
    for encoding in ("cp932", "utf-8"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("cp932", errors="replace")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="generate_a8_main")
    parser.add_argument("--dry-run", action="store_true", help="Validate and score without the 21:00 label")
    parser.add_argument("--execute", action="store_true", help="Write the isolated 21:00 artifact")
    parser.add_argument("--fetch", action="store_true", help="One polite official-card GET")
    parser.add_argument("--board-json", type=Path, default=None)
    parser.add_argument("--html", type=Path, default=None)
    parser.add_argument("--market-null-json", type=Path, default=DEFAULT_MARKET_NULL_JSON)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--official-url", default=OFFICIAL_CARD_URL)
    return parser.parse_args(argv)


def resolve_output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir is not None:
        return args.output_dir
    if args.execute:
        return DEFAULT_EXECUTE_DIR
    return DEFAULT_DRY_RUN_DIR


def load_board(args: argparse.Namespace, output_dir: Path) -> tuple[list[BoardEntry], str]:
    if args.fetch:
        if not args.execute:
            raise GenerationError("--fetch is allowed only with --execute")
        raw = fetch_official_card(args.official_url)
        html_path = output_dir / "official-card.html"
        html_path.write_bytes(raw)
        return parse_official_card_html(decode_official_html(raw)), args.official_url
    if args.html is not None:
        return parse_official_card_html(args.html.read_text(encoding="utf-8")), str(args.html)
    board_path = DEFAULT_BOARD_JSON if args.board_json is None else args.board_json
    return load_board_json(board_path), str(board_path)


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_report(
    *,
    generated_at: datetime,
    source: str,
    entries: Sequence[BoardEntry],
    check: BoardCheck,
    market_by_umaban: Mapping[int, Mapping[str, float | None]],
    predictions: Sequence[Mapping[str, object]],
    coverage: Mapping[str, object],
    quality: Mapping[str, object],
    output_dir: Path,
    execute: bool,
) -> dict[str, object]:
    return {
        "generatedAtJst": generated_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "race": "2026/08/16/A8/04",
        "mode": "execute" if execute else "dry-run",
        "source": source,
        "board": {
            "validFullBoard": check.valid,
            "runnerCount": check.runner_count,
            "positiveFiniteOdds": check.positive_finite_odds,
            "competitionRanking": check.competition_ranking,
            "oddsNondecreasing": check.odds_nondecreasing,
            "sameRankSameOdds": check.same_rank_same_odds,
            "sortedRanks": list(check.sorted_ranks),
            "failReasons": list(check.fail_reasons),
            "entries": [asdict(entry) for entry in entries],
        },
        "coverage": dict(coverage),
        "predictionQuality": dict(quality),
        "marketInjection": {
            "boardDerivedCount": len(BOARD_DERIVED_MARKET),
            "similarRaceLeftNull": sorted(SIMILAR_RACE_MARKET),
            "byUmaban": {str(umaban): dict(values) for umaban, values in market_by_umaban.items()},
        },
        "predictions": list(predictions),
        "writes": {"postgres": False, "r2": False, "cache": False, "outputDir": str(output_dir)},
    }


def run(args: argparse.Namespace, stdout: TextIO = sys.stdout) -> int:
    if args.dry_run and args.execute:
        raise GenerationError("choose either --dry-run or --execute")
    if not args.dry_run and not args.execute:
        args.dry_run = True
    generated_at = now_jst()
    output_dir = resolve_output_dir(args)
    output_dir.mkdir(parents=True, exist_ok=True)
    entries, source = load_board(args, output_dir)
    check = check_board(entries)
    if not check.valid:
        report = {
            "generatedAtJst": generated_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "mode": "execute" if args.execute else "dry-run",
            "source": source,
            "board": asdict(check) | {"entries": [asdict(entry) for entry in entries]},
            "decision": "DO_NOT_GENERATE",
            "writes": {"postgres": False, "r2": False, "cache": False},
        }
        write_json(output_dir / "a8-board-invalid.json", report)
        stdout.write(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        return 2

    market_by_umaban = production_market_features(entries)
    market_null = json.loads(args.market_null_json.read_text(encoding="utf-8"))
    if not isinstance(market_null, dict):
        raise GenerationError("market-null JSON root is not an object")
    overlaid = overlay_market_features(market_null, market_by_umaban)
    model_path = args.model_dir / "model.json"
    metadata = json.loads((args.model_dir / "metadata.json").read_text(encoding="utf-8"))
    feature_names = metadata.get("feature_names")
    if not isinstance(feature_names, list) or len(feature_names) != FEATURE_COUNT:
        raise GenerationError("model metadata feature_names must contain 250 names")
    names = [str(name) for name in feature_names]
    runners = overlaid["runners"]
    if not isinstance(runners, list):
        raise GenerationError("overlaid runners is not a list")
    predictions = score_runners(runners, names, model_path)
    typed_runners = [runner for runner in runners if isinstance(runner, dict)]
    if len(typed_runners) != len(runners):
        raise GenerationError("overlaid runners contain a non-object")
    coverage = feature_coverage(typed_runners)
    quality = softmax_quality(predictions)
    summary = {
        "race": "2026/08/16/A8/04",
        "label": (
            "EXPERIMENTAL / MARKET-OVERLAY / OUT-OF-DISTRIBUTION / NO PRODUCTION WRITE"
            if args.execute
            else "DRY-RUN / MARKET-OVERLAY / NO PRODUCTION WRITE"
        ),
        "modelVersion": metadata.get("model_version"),
        "featureCount": FEATURE_COUNT,
        "nonnullByUmaban": coverage["nonnullByUmaban"],
        "nonnullLabel": coverage["nonnullLabel"],
        "marketFeatureCount": 16,
        "boardDerivedMarketFeatures": 13,
        "similarRaceMarketFeaturesLeftNull": 3,
        "validFullBoard": True,
        "predictionQuality": quality,
        "generatedAt": generated_at.astimezone(timezone.utc).isoformat(),
        "writes": {"postgres": False, "r2": False, "cache": False},
        "boardSource": source,
    }
    full = {
        "summary": summary,
        "featureNames": names,
        "marketFeatures": list(MARKET16),
        "predictions": predictions,
        "runners": runners,
        "board": [asdict(entry) for entry in entries],
    }
    write_json(output_dir / "a8-market-overlay-prediction-full.json", full)
    write_json(
        output_dir / "a8-market-overlay-prediction-summary.json",
        {"summary": summary, "predictions": predictions},
    )
    report = build_report(
        generated_at=generated_at,
        source=source,
        entries=entries,
        check=check,
        market_by_umaban=market_by_umaban,
        predictions=predictions,
        coverage=coverage,
        quality=quality,
        output_dir=output_dir,
        execute=bool(args.execute),
    )
    write_json(output_dir / "a8-main-report.json", report)
    stdout.write(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except (BoardInvalidError, GenerationError, OSError, json.JSONDecodeError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
