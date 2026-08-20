"""Identity-free overseas finish-position features and ranking.

Japanese JV catalogue codes are missing for most foreign runners. Presence
flags for those codes are therefore a Japan-horse proxy and must not enter
the scorer. Market features are used only when every runner on the card has
a valid price, so a half-filled JV odds column cannot revive that bias.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import numpy as np
import pandas as pd

FEATURE_COLUMNS: tuple[str, ...] = (
    "weight_kg",
    "age",
    "sex",
    "distance",
    "track_code",
    "grade_code",
    "field_size",
    "weight_per_field",
    "weight_vs_field",
    "age_vs_field",
    "is_three_year_old",
    "is_female",
    "weight_rank",
    "age_rank",
    "market_available",
    "log_odds",
    "implied_prob",
    "ninki",
)

IDENTITY_FEATURE_COLUMNS: tuple[str, ...] = (
    "has_jockey",
    "has_trainer",
    "has_horse_reg",
)

GRADE_MAP: dict[str, int] = {"A": 1, "B": 2, "C": 3}
UNKNOWN_GRADE_CODE: int = 4
JOCKEY_PLACEHOLDER: str = "00000"
TRAINER_PLACEHOLDER: str = "00000"
HORSE_REG_PLACEHOLDER: str = "0000000000"
WEIGHT_SCALE: float = 10.0
MIN_VALID_ODDS: float = 1.01
FEMALE_SEX_CODE: int = 2
THREE_YEAR_OLD: int = 3
TANSHO_ODDS_COLUMN: str = "tansho_odds"
TANSHO_NINKI_COLUMN: str = "tansho_ninkijun"
NETKEIBA_SOURCE: str = "netkeiba"
FORM_STARTS_COLUMN: str = "form_starts"
FORM_WINS_COLUMN: str = "form_wins"
FORM_AVG_FINISH_COLUMN: str = "form_avg_finish"
FORM_LAST_FINISH_COLUMN: str = "form_last_finish"
FORM_RECENT_AVG_COLUMN: str = "form_recent_avg_finish"
FORM_LAST_DISTANCE_DELTA_COLUMN: str = "form_last_distance_delta"
FORM_DAYS_SINCE_COLUMN: str = "form_days_since"
RECENT_FORM_WINDOW: int = 3
FORM_RECENT_WEIGHT: float = 0.4
FORM_LAST_WEIGHT: float = 0.2
FORM_AVG_WEIGHT: float = 0.4
BLEND_FORM_WITH_MARKET_WEIGHT: float = 0.32
BLEND_PRIZE_WITH_MARKET_WEIGHT: float = 0.14
BLEND_JOCKEY_WITH_MARKET_WEIGHT: float = 0.14
BLEND_TRAINER_WITH_MARKET_WEIGHT: float = 0.12
BLEND_OWNER_WITH_MARKET_WEIGHT: float = 0.08
BLEND_MARKET_WEIGHT: float = 0.1
BLEND_MODEL_WITH_MARKET_WEIGHT: float = 0.1
BLEND_FORM_WITHOUT_MARKET_WEIGHT: float = 0.35555555555555557
BLEND_PRIZE_WITHOUT_MARKET_WEIGHT: float = 0.15555555555555556
BLEND_JOCKEY_WITHOUT_MARKET_WEIGHT: float = 0.15555555555555556
BLEND_TRAINER_WITHOUT_MARKET_WEIGHT: float = 0.13333333333333333
BLEND_OWNER_WITHOUT_MARKET_WEIGHT: float = 0.08888888888888889
BLEND_MODEL_WITHOUT_MARKET_WEIGHT: float = 0.1111111111111111
SHRINKAGE_STARTS: float = 20.0
DEFAULT_WIN_RATE_PRIOR: float = 0.15
DEFAULT_SHOW_RATE_PRIOR: float = 0.35
PROBABILITY_FLOOR: float = 1e-12
INFORMATIVE_PROB_SPAN: float = 1e-9
FIELD_MEAN_OFFSET: float = 1.0
WIN_FINISH_POSITION: int = 1
SHOW_FINISH_POSITION: int = 3
G1_PURSE_POINTS: float = 100.0
G2_PURSE_POINTS: float = 40.0
G3_PURSE_POINTS: float = 20.0
UNGRADED_PURSE_POINTS: float = 8.0
WIN_PRIZE_SHARE: float = 1.0
SECOND_PRIZE_SHARE: float = 0.38
THIRD_PRIZE_SHARE: float = 0.22
FOURTH_PRIZE_SHARE: float = 0.12
UNPLACED_PRIZE_SHARE: float = 0.03
PERSON_WIN_RATE_WEIGHT: float = 0.6
PERSON_SHOW_RATE_WEIGHT: float = 0.4
PERSON_QUALITY_PRIOR: float = (
    PERSON_WIN_RATE_WEIGHT * DEFAULT_WIN_RATE_PRIOR + PERSON_SHOW_RATE_WEIGHT * DEFAULT_SHOW_RATE_PRIOR
)
PRIZE_MISSING_PRIOR: float = UNGRADED_PURSE_POINTS * UNPLACED_PRIZE_SHARE
FORM_PLACKETT_TEMPERATURE: float = 2.0
MODEL_PLACKETT_TEMPERATURE: float = 2.0
G3_NAME_PATTERN: re.Pattern[str] = re.compile(r"G\s*I{3}|G3|ＧＩＩＩ", re.IGNORECASE)
G2_NAME_PATTERN: re.Pattern[str] = re.compile(r"G\s*I{2}|G2|ＧＩＩ", re.IGNORECASE)
G1_NAME_PATTERN: re.Pattern[str] = re.compile(r"G\s*I\b|G1|ＧＩ", re.IGNORECASE)
PERSON_JOCKEY: str = "jockey"
PERSON_TRAINER: str = "trainer"
PERSON_OWNER: str = "owner"
YYYYMMDD_LENGTH: int = 8
CALENDAR_MONTH_MIN: int = 1
CALENDAR_MONTH_MAX: int = 12
CALENDAR_DAY_MIN: int = 1
CALENDAR_DAY_MAX: int = 31


class FinishRegressor(Protocol):
    def predict(self, features: pd.DataFrame) -> np.ndarray: ...


class ProbabilityModel(Protocol):
    def predict_proba(self, features: pd.DataFrame) -> np.ndarray: ...


@dataclass(frozen=True, slots=True)
class FormSummary:
    starts: int
    wins: int
    avg_finish: float | None
    last_finish: float | None
    last_distance_delta: int | None
    days_since: int | None
    recent_avg_finish: float | None = None
    prize_points: float = 0.0
    prize_per_start: float | None = None
    win_rate: float | None = None


@dataclass(frozen=True, slots=True)
class PersonSummary:
    starts: int
    wins: int
    shows: int
    win_rate: float | None
    show_rate: float | None
    prize_points: float
    prize_per_start: float | None


EMPTY_FORM = FormSummary(
    starts=0,
    wins=0,
    avg_finish=None,
    last_finish=None,
    last_distance_delta=None,
    days_since=None,
    recent_avg_finish=None,
    prize_points=0.0,
    prize_per_start=None,
    win_rate=None,
)

EMPTY_PERSON = PersonSummary(
    starts=0,
    wins=0,
    shows=0,
    win_rate=None,
    show_rate=None,
    prize_points=0.0,
    prize_per_start=None,
)


class InvertedScoreModel:
    """Treat a higher-is-better scorer (LambdaRank) as a finish-position model."""

    def __init__(self, model: FinishRegressor) -> None:
        self._model: FinishRegressor = model

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        return -np.asarray(self._model.predict(features), dtype=float)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def parse_tansho_odds(frame: pd.DataFrame) -> pd.Series:
    if TANSHO_ODDS_COLUMN not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[TANSHO_ODDS_COLUMN], errors="coerce")


def market_is_complete(odds: pd.Series) -> bool:
    if len(odds) == 0:
        return False
    valid = odds.notna() & np.isfinite(odds.to_numpy()) & (odds > MIN_VALID_ODDS)
    return bool(valid.all())


def attach_tansho_odds(
    runners: pd.DataFrame,
    *,
    odds_by_umaban: dict[int, float],
    ninki_by_umaban: dict[int, int] | None = None,
) -> pd.DataFrame:
    """Copy live tansho prices onto the card. Incomplete maps stay unused."""
    umaban = pd.to_numeric(runners["umaban"], errors="coerce")
    odds = umaban.map(odds_by_umaban)
    attached = runners.copy()
    attached[TANSHO_ODDS_COLUMN] = odds
    if ninki_by_umaban is not None:
        attached[TANSHO_NINKI_COLUMN] = umaban.map(ninki_by_umaban)
    return attached


def _as_timestamp(value: object) -> pd.Timestamp | None:
    if value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return pd.Timestamp(year=int(value.year), month=int(value.month), day=int(value.day))
    digits = "".join(character for character in str(value).strip() if character.isdigit())
    if len(digits) < YYYYMMDD_LENGTH:
        return None
    year = int(digits[:4])
    month = int(digits[4:6])
    day = int(digits[6:8])
    if month < CALENDAR_MONTH_MIN or month > CALENDAR_MONTH_MAX:
        return None
    if day < CALENDAR_DAY_MIN or day > CALENDAR_DAY_MAX:
        return None
    try:
        return pd.Timestamp(year=year, month=month, day=day)
    except ValueError:
        return None


def _optional_float(value: object) -> float | None:
    number = pd.to_numeric(pd.Series([value], dtype=object), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    return float(number)


def parse_race_grade(race_name: object) -> int:
    """Map a published race name to G1=1, G2=2, G3=3, else 0."""
    text = str(race_name)
    if G3_NAME_PATTERN.search(text) is not None:
        return 3
    if G2_NAME_PATTERN.search(text) is not None:
        return 2
    if G1_NAME_PATTERN.search(text) is not None:
        return 1
    return 0


def prize_points_for_start(*, finish_position: object, race_name: object) -> float:
    """Grade-weighted purse share. Comparable across JP and foreign runners."""
    grade = parse_race_grade(race_name)
    purse = G1_PURSE_POINTS
    if grade == 2:
        purse = G2_PURSE_POINTS
    elif grade == 3:
        purse = G3_PURSE_POINTS
    elif grade == 0:
        purse = UNGRADED_PURSE_POINTS
    finish = _optional_float(finish_position)
    share = UNPLACED_PRIZE_SHARE
    if finish == 1:
        share = WIN_PRIZE_SHARE
    elif finish == 2:
        share = SECOND_PRIZE_SHARE
    elif finish == 3:
        share = THIRD_PRIZE_SHARE
    elif finish == 4:
        share = FOURTH_PRIZE_SHARE
    return purse * share


def _history_prize_points(work: pd.DataFrame) -> float:
    if work.empty:
        return 0.0
    names = (
        work["race_name"]
        if "race_name" in work.columns
        else pd.Series([""] * len(work), index=work.index)
    )
    finishes = (
        work["finish_position"]
        if "finish_position" in work.columns
        else pd.Series([None] * len(work), index=work.index)
    )
    points = [
        prize_points_for_start(finish_position=finish, race_name=name)
        for finish, name in zip(finishes.to_numpy(), names.to_numpy(), strict=True)
    ]
    return float(sum(points))


def _form_for_umaban(
    umaban_value: object, *, form_by_umaban: dict[int, FormSummary]
) -> FormSummary:
    parsed = pd.to_numeric(pd.Series([umaban_value], dtype=object), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return EMPTY_FORM
    return form_by_umaban.get(int(parsed), EMPTY_FORM)


def summarize_netkeiba_form(
    history: pd.DataFrame,
    *,
    source_horse_id: str,
    race_date: str,
    target_distance: int,
) -> FormSummary:
    """Summarize netkeiba-only starts strictly before the target race date."""
    required = {"source", "source_horse_id", "race_date"}
    if history.empty or not required.issubset(set(history.columns)):
        return EMPTY_FORM
    target = _as_timestamp(race_date)
    if target is None:
        return EMPTY_FORM
    source = history["source"].astype(str).str.strip()
    horse = history["source_horse_id"].astype(str).str.strip()
    race_dates = pd.to_datetime(history["race_date"].map(_as_timestamp), errors="coerce")
    prior = (
        (source == NETKEIBA_SOURCE)
        & (horse == str(source_horse_id).strip())
        & race_dates.notna()
        & (race_dates < target)
    )
    work = history.loc[prior].copy()
    if work.empty:
        return EMPTY_FORM
    work["_race_date"] = race_dates.loc[prior]
    sort_columns = ["_race_date"]
    sort_ascending = [False]
    if "race_day_sequence" in work.columns:
        sort_columns.append("race_day_sequence")
        sort_ascending.append(False)
    work = work.sort_values(by=sort_columns, ascending=sort_ascending, kind="mergesort")
    finishes = (
        pd.to_numeric(work["finish_position"], errors="coerce")
        if "finish_position" in work.columns
        else pd.Series(np.nan, index=work.index, dtype=float)
    )
    valid_finishes = finishes.dropna()
    recent = valid_finishes.iloc[:RECENT_FORM_WINDOW]
    last_row = work.iloc[0]
    last_finish = _optional_float(last_row["finish_position"]) if "finish_position" in work.columns else None
    last_distance = (
        _optional_float(last_row["distance_metres"]) if "distance_metres" in work.columns else None
    )
    last_distance_delta = None if last_distance is None else int(last_distance) - target_distance
    last_date = _as_timestamp(last_row["race_date"])
    days_since = None if last_date is None else int((target - last_date).days)
    starts = int(len(work))
    wins = int((finishes == WIN_FINISH_POSITION).sum())
    prize_points = _history_prize_points(work)
    return FormSummary(
        starts=starts,
        wins=wins,
        avg_finish=None if valid_finishes.empty else float(valid_finishes.mean()),
        last_finish=last_finish,
        last_distance_delta=last_distance_delta,
        days_since=days_since,
        recent_avg_finish=None if recent.empty else float(recent.mean()),
        prize_points=prize_points,
        prize_per_start=None if starts == 0 else prize_points / float(starts),
        win_rate=None if starts == 0 else wins / float(starts),
    )


def attach_netkeiba_form(
    runners: pd.DataFrame,
    *,
    form_by_umaban: dict[int, FormSummary],
) -> pd.DataFrame:
    """Copy per-umaban netkeiba form onto the card."""
    summaries = [
        _form_for_umaban(value, form_by_umaban=form_by_umaban) for value in runners["umaban"].to_numpy()
    ]
    attached = runners.copy()
    attached[FORM_STARTS_COLUMN] = [item.starts for item in summaries]
    attached[FORM_WINS_COLUMN] = [item.wins for item in summaries]
    attached[FORM_AVG_FINISH_COLUMN] = [item.avg_finish for item in summaries]
    attached[FORM_LAST_FINISH_COLUMN] = [item.last_finish for item in summaries]
    attached[FORM_RECENT_AVG_COLUMN] = [item.recent_avg_finish for item in summaries]
    attached[FORM_LAST_DISTANCE_DELTA_COLUMN] = [item.last_distance_delta for item in summaries]
    attached[FORM_DAYS_SINCE_COLUMN] = [item.days_since for item in summaries]
    return attached


def summarize_netkeiba_person(
    history: pd.DataFrame,
    *,
    person_kind: str,
    source_person_id: str,
    race_date: str,
) -> PersonSummary:
    """Netkeiba-only person results strictly before the target race date."""
    required = {"source", "person_kind", "source_person_id", "race_date"}
    if history.empty or not required.issubset(set(history.columns)):
        return EMPTY_PERSON
    target = _as_timestamp(race_date)
    if target is None:
        return EMPTY_PERSON
    source = history["source"].astype(str).str.strip()
    kind = history["person_kind"].astype(str).str.strip()
    person = history["source_person_id"].astype(str).str.strip()
    race_dates = pd.to_datetime(history["race_date"].map(_as_timestamp), errors="coerce")
    prior = (
        (source == NETKEIBA_SOURCE)
        & (kind == person_kind)
        & (person == str(source_person_id).strip())
        & race_dates.notna()
        & (race_dates < target)
    )
    work = history.loc[prior]
    if work.empty:
        return EMPTY_PERSON
    finishes = (
        pd.to_numeric(work["finish_position"], errors="coerce")
        if "finish_position" in work.columns
        else pd.Series(np.nan, index=work.index, dtype=float)
    )
    starts = int(len(work))
    wins = int((finishes == WIN_FINISH_POSITION).sum())
    shows = int((finishes <= SHOW_FINISH_POSITION).sum())
    prize_points = _history_prize_points(work)
    return PersonSummary(
        starts=starts,
        wins=wins,
        shows=shows,
        win_rate=None if starts == 0 else wins / float(starts),
        show_rate=None if starts == 0 else shows / float(starts),
        prize_points=prize_points,
        prize_per_start=None if starts == 0 else prize_points / float(starts),
    )


def empirical_bayes_rate(
    *,
    successes: float,
    trials: float,
    prior_rate: float,
    strength: float = SHRINKAGE_STARTS,
) -> float:
    """Shrink a binomial rate toward a prior. Small n cannot dominate large n."""
    if trials <= 0.0:
        return prior_rate
    return (successes + strength * prior_rate) / (trials + strength)


def normalize_overround(odds: np.ndarray) -> np.ndarray:
    """Convert decimal odds to win probabilities that sum to 1 (take out overround)."""
    prices = np.asarray(odds, dtype=float)
    if prices.size == 0:
        return np.zeros(0, dtype=float)
    implied = 1.0 / np.clip(prices, MIN_VALID_ODDS, None)
    implied[~np.isfinite(implied)] = 0.0
    total = float(implied.sum())
    if total <= 0.0:
        return np.full(len(prices), 1.0 / float(len(prices)), dtype=float)
    return implied / total


def prize_prior_score(summaries: Sequence[FormSummary | PersonSummary]) -> np.ndarray:
    """Lower-is-better finish prior from prize-per-start. Missing stays NaN."""
    scores: list[float] = []
    for summary in summaries:
        if summary.prize_per_start is None:
            scores.append(float("nan"))
            continue
        scores.append(-float(summary.prize_per_start))
    return np.asarray(scores, dtype=float)


def person_quality_scores(summaries: Sequence[PersonSummary]) -> np.ndarray:
    """Empirical-Bayes win/show quality in (0, 1). Missing stays NaN."""
    scores: list[float] = []
    for summary in summaries:
        if summary.win_rate is None and summary.show_rate is None:
            scores.append(float("nan"))
            continue
        win_rate = empirical_bayes_rate(
            successes=float(summary.wins),
            trials=float(summary.starts),
            prior_rate=DEFAULT_WIN_RATE_PRIOR,
        )
        show_rate = empirical_bayes_rate(
            successes=float(summary.shows),
            trials=float(summary.starts),
            prior_rate=DEFAULT_SHOW_RATE_PRIOR,
        )
        scores.append(PERSON_WIN_RATE_WEIGHT * win_rate + PERSON_SHOW_RATE_WEIGHT * show_rate)
    return np.asarray(scores, dtype=float)


def person_prior_score(summaries: Sequence[PersonSummary]) -> np.ndarray:
    """Lower-is-better prior from shrunk win/show rates. Missing stays NaN."""
    return 1.0 - person_quality_scores(summaries)


def _people_for_umaban(
    umaban_value: object,
    *,
    people_by_umaban: dict[int, PersonSummary],
) -> PersonSummary:
    parsed = pd.to_numeric(pd.Series([umaban_value], dtype=object), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return EMPTY_PERSON
    return people_by_umaban.get(int(parsed), EMPTY_PERSON)


def _neutral_field_mean(form_frame: pd.DataFrame, known_scores: np.ndarray) -> float:
    field_column = "field_size_raw" if "field_size_raw" in form_frame.columns else "field_size"
    if field_column in form_frame.columns:
        field = pd.to_numeric(form_frame[field_column], errors="coerce")
        if field.notna().any():
            return (float(field.dropna().iloc[0]) + FIELD_MEAN_OFFSET) / 2.0
    if known_scores.size > 0:
        return float(np.mean(known_scores))
    return 0.0


def _form_column(form_frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in form_frame.columns:
        return pd.Series(np.nan, index=form_frame.index, dtype=float)
    return pd.to_numeric(form_frame[column], errors="coerce")


def _weighted_form_row(
    *,
    recent: float | None,
    last: float | None,
    avg: float | None,
) -> float | None:
    parts: list[float] = []
    weights: list[float] = []
    if recent is not None:
        parts.append(recent)
        weights.append(FORM_RECENT_WEIGHT)
    if last is not None:
        parts.append(last)
        weights.append(FORM_LAST_WEIGHT)
    if avg is not None:
        parts.append(avg)
        weights.append(FORM_AVG_WEIGHT)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    total = float(sum(weights))
    return float(sum(part * weight for part, weight in zip(parts, weights, strict=True)) / total)


def form_prior_score(form_frame: pd.DataFrame) -> np.ndarray:
    """Lower-is-better finish prior. Missing form uses a shared field mean."""
    if len(form_frame) == 0:
        return np.zeros(0, dtype=float)
    recent = _form_column(form_frame, FORM_RECENT_AVG_COLUMN)
    last = _form_column(form_frame, FORM_LAST_FINISH_COLUMN)
    avg = _form_column(form_frame, FORM_AVG_FINISH_COLUMN)
    scores = [
        _weighted_form_row(
            recent=None if pd.isna(recent_value) else float(recent_value),
            last=None if pd.isna(last_value) else float(last_value),
            avg=None if pd.isna(avg_value) else float(avg_value),
        )
        for recent_value, last_value, avg_value in zip(recent, last, avg, strict=True)
    ]
    score = np.array(
        [np.nan if value is None else value for value in scores],
        dtype=float,
    )
    known = score[np.isfinite(score)]
    score[~np.isfinite(score)] = _neutral_field_mean(form_frame, known)
    return score


def _softmax(scores: np.ndarray) -> np.ndarray:
    work = np.asarray(scores, dtype=float)
    if work.size == 0:
        return np.zeros(0, dtype=float)
    shifted = work - float(np.max(work))
    exp = np.exp(shifted)
    total = float(exp.sum())
    if total <= 0.0:
        return np.full(len(work), 1.0 / float(len(work)), dtype=float)
    return exp / total


def normalize_positive_scores(values: np.ndarray, *, missing_prior: float) -> np.ndarray:
    """Turn non-negative ability scores into win probabilities (sum to 1).

    Missing or non-positive entries use ``missing_prior``. This keeps
    magnitude: 0.23 vs 0.25 stays near 50/50, unlike a card z-score.
    """
    work = np.asarray(values, dtype=float)
    if work.size == 0:
        return np.zeros(0, dtype=float)
    filled = np.where(np.isfinite(work) & (work > 0.0), work, missing_prior)
    total = float(filled.sum())
    if total <= 0.0:
        return np.full(len(work), 1.0 / float(len(work)), dtype=float)
    return filled / total


def plackett_luce_from_cost(values: np.ndarray, *, temperature: float) -> np.ndarray:
    """Plackett–Luce / multinomial logit: p_i ∝ exp(-cost_i / τ).

    A fixed temperature keeps native units. Card-wise z-scoring would map
    any two distinct values to the same 88/12 split.
    """
    work = np.asarray(values, dtype=float)
    if work.size == 0:
        return np.zeros(0, dtype=float)
    missing = ~np.isfinite(work)
    if bool(missing.all()):
        return np.full(len(work), 1.0 / float(len(work)), dtype=float)
    fill = float(np.mean(work[~missing]))
    filled = np.where(missing, fill, work)
    return _softmax(-filled / temperature)


def _probs_from_cost(
    values: np.ndarray | None,
    *,
    size: int,
    temperature: float,
) -> np.ndarray:
    if values is None:
        return np.full(size, 1.0 / float(max(size, 1)), dtype=float)
    return plackett_luce_from_cost(values, temperature=temperature)


def _probs_from_positive(
    values: np.ndarray | None,
    *,
    size: int,
    missing_prior: float,
) -> np.ndarray:
    if values is None:
        return np.full(size, 1.0 / float(max(size, 1)), dtype=float)
    return normalize_positive_scores(values, missing_prior=missing_prior)


def _positive_from_lower_is_better(values: np.ndarray | None) -> np.ndarray | None:
    if values is None:
        return None
    return -np.asarray(values, dtype=float)


def _quality_from_person_cost(values: np.ndarray | None) -> np.ndarray | None:
    if values is None:
        return None
    return 1.0 - np.asarray(values, dtype=float)


def _is_informative_probs(probs: np.ndarray) -> bool:
    if probs.size == 0:
        return False
    return float(np.max(probs) - np.min(probs)) > INFORMATIVE_PROB_SPAN


def log_opinion_pool(prob_rows: Sequence[np.ndarray], weights: Sequence[float]) -> np.ndarray:
    """Logarithmic opinion pool (Benter / externally Bayesian combination)."""
    active = [
        (np.asarray(probs, dtype=float), float(weight))
        for probs, weight in zip(prob_rows, weights, strict=True)
        if weight > 0.0 and _is_informative_probs(np.asarray(probs, dtype=float))
    ]
    if not active:
        if not prob_rows:
            return np.zeros(0, dtype=float)
        size = len(prob_rows[0])
        return np.full(size, 1.0 / float(max(size, 1)), dtype=float)
    size = len(active[0][0])
    total_weight = sum(weight for _, weight in active)
    log_p = np.zeros(size, dtype=float)
    for probs, weight in active:
        clipped = np.clip(probs, PROBABILITY_FLOOR, 1.0)
        clipped = clipped / float(clipped.sum())
        log_p = log_p + (weight / total_weight) * np.log(clipped)
    pooled = np.exp(log_p)
    return pooled / float(pooled.sum())


def viewpoint_win_probabilities(
    *,
    model_finish: np.ndarray,
    form_finish: np.ndarray,
    market_finish: np.ndarray,
    market_available: bool,
    prize_finish: np.ndarray | None = None,
    jockey_finish: np.ndarray | None = None,
    trainer_finish: np.ndarray | None = None,
    owner_finish: np.ndarray | None = None,
    market_odds: np.ndarray | None = None,
) -> tuple[tuple[np.ndarray, ...], tuple[float, ...]]:
    """One probability vector per viewpoint. Each finite vector sums to 1.

    Form and the fitted model use Plackett–Luce on native costs (Bolton–
    Chapman / Henery). Prize and people rates are L1-normalized abilities
    after empirical-Bayes shrinkage. Market takes out overround.
    """
    model = np.asarray(model_finish, dtype=float)
    form = np.asarray(form_finish, dtype=float)
    size = int(model.size)
    if market_available and market_odds is not None:
        market_probs = normalize_overround(np.asarray(market_odds, dtype=float))
    else:
        market_probs = _probs_from_cost(
            np.asarray(market_finish, dtype=float) if market_available else None,
            size=size,
            temperature=FORM_PLACKETT_TEMPERATURE,
        )
    components = (
        _probs_from_cost(form, size=size, temperature=FORM_PLACKETT_TEMPERATURE),
        _probs_from_positive(
            _positive_from_lower_is_better(prize_finish),
            size=size,
            missing_prior=PRIZE_MISSING_PRIOR,
        ),
        _probs_from_positive(
            _quality_from_person_cost(jockey_finish),
            size=size,
            missing_prior=PERSON_QUALITY_PRIOR,
        ),
        _probs_from_positive(
            _quality_from_person_cost(trainer_finish),
            size=size,
            missing_prior=PERSON_QUALITY_PRIOR,
        ),
        _probs_from_positive(
            _quality_from_person_cost(owner_finish),
            size=size,
            missing_prior=PERSON_QUALITY_PRIOR,
        ),
        market_probs,
        _probs_from_cost(
            model if regressor_is_informative(model) else None,
            size=size,
            temperature=MODEL_PLACKETT_TEMPERATURE,
        ),
    )
    if market_available:
        weights = (
            BLEND_FORM_WITH_MARKET_WEIGHT,
            BLEND_PRIZE_WITH_MARKET_WEIGHT,
            BLEND_JOCKEY_WITH_MARKET_WEIGHT,
            BLEND_TRAINER_WITH_MARKET_WEIGHT,
            BLEND_OWNER_WITH_MARKET_WEIGHT,
            BLEND_MARKET_WEIGHT,
            BLEND_MODEL_WITH_MARKET_WEIGHT,
        )
    else:
        weights = (
            BLEND_FORM_WITHOUT_MARKET_WEIGHT,
            BLEND_PRIZE_WITHOUT_MARKET_WEIGHT,
            BLEND_JOCKEY_WITHOUT_MARKET_WEIGHT,
            BLEND_TRAINER_WITHOUT_MARKET_WEIGHT,
            BLEND_OWNER_WITHOUT_MARKET_WEIGHT,
            0.0,
            BLEND_MODEL_WITHOUT_MARKET_WEIGHT,
        )
    return components, weights


def blend_predicted_finish(
    *,
    model_finish: np.ndarray,
    form_finish: np.ndarray,
    market_finish: np.ndarray,
    market_available: bool,
    prize_finish: np.ndarray | None = None,
    jockey_finish: np.ndarray | None = None,
    trainer_finish: np.ndarray | None = None,
    owner_finish: np.ndarray | None = None,
    market_odds: np.ndarray | None = None,
) -> np.ndarray:
    """Combine sources in probability space. Market is 10% when complete.

    Each source becomes a card probability; the log opinion pool (Benter)
    mixes them. Predicted finish is -log(p) so a higher win chance ranks
    first.
    """
    model = np.asarray(model_finish, dtype=float)
    if model.size == 0:
        return np.zeros(0, dtype=float)
    components, weights = viewpoint_win_probabilities(
        model_finish=model,
        form_finish=form_finish,
        market_finish=market_finish,
        market_available=market_available,
        prize_finish=prize_finish,
        jockey_finish=jockey_finish,
        trainer_finish=trainer_finish,
        owner_finish=owner_finish,
        market_odds=market_odds,
    )
    pooled = log_opinion_pool(components, weights)
    return -np.log(np.clip(pooled, PROBABILITY_FLOOR, 1.0))


def regressor_is_informative(model_finish: np.ndarray) -> bool:
    finite = model_finish[np.isfinite(model_finish)]
    if finite.size == 0:
        return False
    return bool(np.unique(np.round(finite, 8)).size > 1)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build features that exist for every overseas runner.

    Catalogue-code presence is intentionally omitted: non-zero
    ``kishu_code`` / ``chokyoshi_code`` / ``ketto_toroku_bango`` mostly
    mark Japanese-catalogued horses.
    """
    weight_kg = _numeric_series(df, "weight_raw") / WEIGHT_SCALE
    age = _numeric_series(df, "age_raw")
    sex = _numeric_series(df, "sex")
    field_size = _numeric_series(df, "field_size_raw")
    weight_mean = float(weight_kg.mean())
    age_mean = float(age.mean())
    odds = parse_tansho_odds(df)
    complete_market = market_is_complete(odds)
    if complete_market:
        log_odds = np.log(odds.to_numpy(dtype=float))
        implied_prob = normalize_overround(odds.to_numpy(dtype=float))
        if TANSHO_NINKI_COLUMN in df.columns:
            ninki = pd.to_numeric(df[TANSHO_NINKI_COLUMN], errors="coerce")
            ninki = ninki.fillna(odds.rank(method="first", ascending=True))
        else:
            ninki = odds.rank(method="first", ascending=True)
        market_available = 1.0
    else:
        log_odds = np.zeros(len(df), dtype=float)
        implied_prob = np.zeros(len(df), dtype=float)
        ninki = np.zeros(len(df), dtype=float)
        market_available = 0.0
    out = pd.DataFrame(
        {
            "weight_kg": weight_kg,
            "age": age,
            "sex": sex,
            "distance": _numeric_series(df, "kyori"),
            "track_code": _numeric_series(df, "track_code"),
            "grade_code": df["grade"]
            .map(lambda grade: GRADE_MAP.get(str(grade).strip(), UNKNOWN_GRADE_CODE))
            .astype(int),
            "field_size": field_size,
            "weight_per_field": weight_kg / field_size,
            "weight_vs_field": weight_kg - weight_mean,
            "age_vs_field": age - age_mean,
            "is_three_year_old": (age == THREE_YEAR_OLD).astype(int),
            "is_female": (sex == FEMALE_SEX_CODE).astype(int),
            "weight_rank": weight_kg.rank(method="average", ascending=True),
            "age_rank": age.rank(method="average", ascending=True),
            "market_available": market_available,
            "log_odds": log_odds,
            "implied_prob": implied_prob,
            "ninki": ninki,
        }
    )
    return out[list(FEATURE_COLUMNS)]


def identity_completeness_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Legacy Japan-catalogue presence flags. Not used for scoring."""
    kishu = df["kishu_code"].astype(str).str.strip()
    trainer = df["chokyoshi_code"].astype(str).str.strip()
    horse = df["ketto_toroku_bango"].astype(str).str.strip()
    return pd.DataFrame(
        {
            "has_jockey": (kishu != JOCKEY_PLACEHOLDER).astype(int),
            "has_trainer": (trainer != TRAINER_PLACEHOLDER).astype(int),
            "has_horse_reg": (horse != HORSE_REG_PLACEHOLDER).astype(int),
        }
    )


def predicted_finish_from_features(
    features: pd.DataFrame,
    *,
    regressor: FinishRegressor | None = None,
) -> np.ndarray:
    if regressor is not None:
        return np.asarray(regressor.predict(features[list(FEATURE_COLUMNS)]), dtype=float)
    if len(features) == 0:
        return np.zeros(0, dtype=float)
    # No model: cheaper market (lower log odds) ranks ahead when complete.
    if float(features["market_available"].iloc[0]) == 1.0:
        return features["log_odds"].to_numpy(dtype=float)
    return np.zeros(len(features), dtype=float)


def rank_overseas_card(*, umaban: pd.Series, predicted_finish: np.ndarray) -> pd.Series:
    """Lower predicted finish is better.

    Exact ties break toward higher umaban so a Japanese-ID pair sitting at
    umaban 3/4 cannot take ranks 1–2 from row order alone.
    """
    work = pd.DataFrame(
        {
            "umaban": pd.to_numeric(umaban, errors="coerce"),
            "predicted_finish": predicted_finish,
        }
    )
    ordered = work.sort_values(
        by=["predicted_finish", "umaban"],
        ascending=[True, False],
        kind="mergesort",
    )
    ranks = pd.Series(np.arange(1, len(ordered) + 1, dtype=int), index=ordered.index)
    return ranks.reindex(work.index)


def _class_probability(
    features: pd.DataFrame,
    *,
    model: ProbabilityModel | None,
) -> np.ndarray:
    if model is None:
        return np.zeros(len(features), dtype=float)
    proba = np.asarray(model.predict_proba(features[list(FEATURE_COLUMNS)]))
    if proba.ndim != 2 or proba.shape[1] < 2:
        return np.zeros(len(features), dtype=float)
    return proba[:, 1]


def score_overseas_card(
    runners: pd.DataFrame,
    *,
    regressor: FinishRegressor | None = None,
    clf_top1: ProbabilityModel | None = None,
    clf_top3: ProbabilityModel | None = None,
    form_by_umaban: dict[int, FormSummary] | None = None,
    jockey_by_umaban: dict[int, PersonSummary] | None = None,
    trainer_by_umaban: dict[int, PersonSummary] | None = None,
    owner_by_umaban: dict[int, PersonSummary] | None = None,
) -> pd.DataFrame:
    """Score a card with the shipped identity-free feature set."""
    features = engineer_features(runners)
    # Without a fitted regressor, skip the log-odds fallback. That value is
    # already the market board and would count the same price twice.
    model_finish = (
        predicted_finish_from_features(features, regressor=regressor)
        if regressor is not None
        else np.zeros(len(features), dtype=float)
    )
    forms_lookup = {} if form_by_umaban is None else form_by_umaban
    form_frame = attach_netkeiba_form(runners, form_by_umaban=forms_lookup)
    form_finish = form_prior_score(form_frame)
    umaban_values = runners["umaban"].to_numpy()
    forms = [_form_for_umaban(value, form_by_umaban=forms_lookup) for value in umaban_values]
    jockeys = [
        _people_for_umaban(value, people_by_umaban={} if jockey_by_umaban is None else jockey_by_umaban)
        for value in umaban_values
    ]
    trainers = [
        _people_for_umaban(value, people_by_umaban={} if trainer_by_umaban is None else trainer_by_umaban)
        for value in umaban_values
    ]
    owners = [
        _people_for_umaban(value, people_by_umaban={} if owner_by_umaban is None else owner_by_umaban)
        for value in umaban_values
    ]
    market_available = False
    market_finish = np.zeros(len(features), dtype=float)
    market_odds: np.ndarray | None = None
    if len(features) > 0 and float(features["market_available"].iloc[0]) == 1.0:
        market_available = True
        market_finish = features["ninki"].to_numpy(dtype=float)
        market_odds = parse_tansho_odds(runners).to_numpy(dtype=float)
    pred_finish = blend_predicted_finish(
        model_finish=model_finish,
        form_finish=form_finish,
        market_finish=market_finish,
        market_available=market_available,
        prize_finish=prize_prior_score(forms),
        jockey_finish=person_prior_score(jockeys),
        trainer_finish=person_prior_score(trainers),
        owner_finish=person_prior_score(owners),
        market_odds=market_odds,
    )
    pred_score = -pred_finish
    ranks = rank_overseas_card(umaban=runners["umaban"], predicted_finish=pred_finish)
    ketto = runners["ketto_toroku_bango"].astype(str)
    umaban = pd.to_numeric(runners["umaban"], errors="coerce").astype(int)
    unknown = ketto.str.strip() == HORSE_REG_PLACEHOLDER
    ketto = ketto.mask(unknown, "UMABAN_" + umaban.astype(str).str.zfill(2))
    result = pd.DataFrame(
        {
            "umaban": umaban,
            "ketto_toroku_bango": ketto,
            "predicted_finish_position": pred_finish,
            "predicted_score": pred_score,
            "predicted_rank": ranks.astype(int),
            "predicted_top1_prob": _class_probability(features, model=clf_top1),
            "predicted_top3_prob": _class_probability(features, model=clf_top3),
            "grade": runners["grade"].astype(str).str.strip(),
            "track_code": runners["track_code"].astype(str),
        }
    )
    return result.sort_values("predicted_rank").reset_index(drop=True)
