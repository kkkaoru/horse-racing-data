"""Arc-only ranking with event-time features and expanding-window evaluation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Literal

import numpy as np
import pandas as pd
from xgboost import XGBRanker, XGBRegressor

from overseas_finish_features import summarize_netkeiba_form


@dataclass(frozen=True)
class TrainingConfig:
    objective: Literal["rank:pairwise", "rank:ndcg", "reg:squarederror"] = (
        "rank:pairwise"
    )
    n_estimators: int = 120
    max_depth: int = 2
    min_child_weight: int = 5
    reg_lambda: int = 10


DEFAULT_CONFIG: TrainingConfig = TrainingConfig()
PERSON_KINDS: tuple[str, ...] = ("jockey", "trainer", "owner")
FEATURES: tuple[str, ...] = (
    "horse_starts",
    "horse_wins",
    "horse_avg_finish",
    "horse_last_finish",
    "horse_recent_finish",
    "horse_days_since",
    "horse_distance_delta",
    "horse_prize_per_start",
    "horse_win_rate",
    "jockey_starts",
    "jockey_win_rate",
    "jockey_show_rate",
    "trainer_starts",
    "trainer_win_rate",
    "trainer_show_rate",
    "owner_starts",
    "owner_win_rate",
    "owner_show_rate",
)
WITHDRAWN: frozenset[str] = frozenset({"取", "取消", "除", "除外"})
EVALUATION_YEARS: tuple[int, ...] = (2023, 2024, 2025)


def unordered_topk(
    predicted: Sequence[str], actual: Sequence[str]
) -> list[dict[str, int | float | bool]]:
    """Report overlap/k and exact-set hits; order inside each selected set is ignored."""
    if len(predicted) < 5 or len(actual) < 5:
        raise ValueError("At least five predicted and actual finishers are required")
    if len(set(predicted)) != len(predicted) or len(set(actual)) != len(actual):
        raise ValueError("Runner identities must be unique")
    if not set(actual).issubset(predicted):
        raise ValueError("Actual finishers must belong to the predicted field")
    result: list[dict[str, int | float | bool]] = []
    for k in range(1, 6):
        hits = len(set(predicted[:k]) & set(actual[:k]))
        result.append(
            {"k": k, "hits": hits, "precision": hits / k, "exact_set_hit": hits == k}
        )
    return result


def person_features(
    daily: pd.DataFrame, *, person_id: str, kind: str, before: str
) -> list[float]:
    """Aggregate only prior event dates; missing identity remains missing, not guessed."""
    if not person_id:
        return [0.0, float("nan"), float("nan")]
    prior = daily.loc[
        (daily["person_kind"] == kind)
        & (daily["source_person_id"] == person_id)
        & (daily["race_date"] < before)
    ]
    starts = float(prior["starts"].sum())
    if starts == 0:
        return [0.0, float("nan"), float("nan")]
    return [
        starts,
        float(prior["wins"].sum()) / starts,
        float(prior["shows"].sum()) / starts,
    ]


def entry_features(
    history: pd.DataFrame,
    *,
    horse_id: str,
    race_date: str,
    people_ids: Mapping[str, str],
    people_daily: pd.DataFrame,
) -> list[float]:
    date.fromisoformat(race_date)
    starts = history.loc[~history["finish_position_text"].isin(WITHDRAWN)]
    form = summarize_netkeiba_form(
        starts,
        source_horse_id=horse_id,
        race_date=race_date,
        target_distance=2400,
    )
    values = [
        form.starts,
        form.wins,
        form.avg_finish,
        form.last_finish,
        form.recent_avg_finish,
        form.days_since,
        form.last_distance_delta,
        form.prize_per_start,
        form.win_rate,
    ]
    features = [float("nan") if value is None else float(value) for value in values]
    for kind in PERSON_KINDS:
        features.extend(
            person_features(
                people_daily,
                person_id=people_ids.get(kind, ""),
                kind=kind,
                before=race_date,
            )
        )
    return features


def entry_people(
    identities: pd.DataFrame, *, horse_id: str, race_date: str
) -> dict[str, str]:
    rows = identities.loc[
        (identities["source_horse_id"] == horse_id)
        & (identities["race_date"] == race_date)
    ]
    people: dict[str, str] = {}
    for kind, group in rows.groupby("person_kind"):
        ids = group["source_person_id"].unique()
        if len(ids) != 1:
            raise ValueError(f"Ambiguous {kind} identity for {horse_id} on {race_date}")
        people[str(kind)] = str(ids[0])
    return people


def card_frame(
    card: pd.DataFrame,
    *,
    history: pd.DataFrame,
    identities: pd.DataFrame,
    people_daily: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, str | float]] = []
    for entry in card.itertuples(index=False):
        horse_id = str(entry.source_horse_id)
        race_date = str(entry.race_date)
        features = entry_features(
            history.loc[history["source_horse_id"] == horse_id],
            horse_id=horse_id,
            race_date=race_date,
            people_ids=entry_people(identities, horse_id=horse_id, race_date=race_date),
            people_daily=people_daily,
        )
        finish = pd.to_numeric(
            pd.Series([entry.finish_position]), errors="coerce"
        ).iloc[0]
        row: dict[str, str | float] = dict(zip(FEATURES, features, strict=True))
        row.update(
            {
                "horse_id": horse_id,
                "race_date": race_date,
                "finish": float(finish) if pd.notna(finish) else float(len(card) + 1),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_dataset(
    history: pd.DataFrame,
    *,
    identities: pd.DataFrame,
    people_daily: pd.DataFrame,
    expected_fields: Mapping[str, int],
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Use complete fields only. DNF/DQ remain runners; explicit withdrawals do not."""
    arc = history.loc[
        history["race_name"].eq("凱旋門賞(GI)")
        & ~history["finish_position_text"].isin(WITHDRAWN)
    ]
    if arc.duplicated(["race_date", "source_horse_id"]).any():
        raise ValueError("Duplicate Arc race/horse rows require source reconciliation")
    cards: list[pd.DataFrame] = []
    excluded: dict[str, str] = {}
    for race_date, card in arc.groupby("race_date", sort=True):
        day = str(race_date)
        top5 = sorted(int(x) for x in card["finish_position"].dropna() if 1 <= x <= 5)
        if len(card) != expected_fields.get(day) or top5 != [1, 2, 3, 4, 5]:
            excluded[day] = "Incomplete field or top-five ground truth"
            continue
        cards.append(
            card_frame(
                card, history=history, identities=identities, people_daily=people_daily
            )
        )
    if not cards:
        raise ValueError("No complete Arc training fields")
    data = pd.concat(cards, ignore_index=True).sort_values(["race_date", "horse_id"])
    available = set(data["race_date"].str[:4])
    if not {str(year) for year in EVALUATION_YEARS}.issubset(available):
        raise ValueError("Every evaluation year must have a complete field")
    return data, excluded


def train_before(
    data: pd.DataFrame,
    *,
    cutoff: str,
    config: TrainingConfig = DEFAULT_CONFIG,
) -> XGBRanker | XGBRegressor:
    """Fit only prior races; optional configurations support audited win optimization."""
    date.fromisoformat(cutoff)
    train = data.loc[data["race_date"] < cutoff].sort_values(["race_date", "horse_id"])
    if train["race_date"].nunique() < 2:
        raise ValueError("At least two prior Arc races are required")
    groups = train.groupby("race_date", sort=False).size().to_numpy()
    field_size = train.groupby("race_date")["horse_id"].transform("size")
    relevance = (field_size + 1 - train["finish"]).clip(lower=0)
    parameters = {
        "n_estimators": config.n_estimators,
        "max_depth": config.max_depth,
        "learning_rate": 0.05,
        "min_child_weight": config.min_child_weight,
        "reg_lambda": config.reg_lambda,
        "random_state": 42,
        "n_jobs": 2,
        "tree_method": "hist",
    }
    if config.objective == "reg:squarederror":
        win_model = XGBRegressor(objective=config.objective, **parameters)
        win_model.fit(train.loc[:, list(FEATURES)], (train["finish"] == 1).astype(int))
        return win_model
    model = XGBRanker(objective=config.objective, **parameters)
    model.fit(train.loc[:, list(FEATURES)], relevance, group=groups)
    return model


def rerank_top_two(base: pd.DataFrame, win_head: pd.DataFrame) -> pd.DataFrame:
    """Only change Top1 selection; every unordered Top2+ set stays identical."""
    if (
        len(base) < 2
        or base["horse_id"].duplicated().any()
        or win_head["horse_id"].duplicated().any()
        or set(base["horse_id"]) != set(win_head["horse_id"])
    ):
        raise ValueError("Reranking requires matching unique fields")
    scored = base.assign(
        win_score=base["horse_id"].map(win_head.set_index("horse_id")["score"])
    )
    if not np.isfinite(scored["win_score"]).all():
        raise ValueError("Win head produced nonfinite scores")
    # ponytail: fixed Top2 gate preserves all larger sets; changing the gate needs new validation.
    first_two = scored.iloc[:2].sort_values(
        ["win_score", "horse_id"], ascending=[False, True]
    )
    return pd.concat([first_two, scored.iloc[2:]], ignore_index=True)


def predict_card(model: XGBRanker | XGBRegressor, card: pd.DataFrame) -> pd.DataFrame:
    if (
        card.empty
        or card["race_date"].nunique() != 1
        or card["horse_id"].duplicated().any()
    ):
        raise ValueError("Prediction requires one nonempty race with unique horses")
    scores = np.asarray(model.predict(card.loc[:, list(FEATURES)]), dtype=float)
    if not np.isfinite(scores).all():
        raise ValueError("Model produced nonfinite scores")
    return card.assign(score=scores).sort_values(
        ["score", "horse_id"], ascending=[False, True]
    )
