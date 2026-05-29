# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false
"""Race-internal field features for running-style prediction.

Mirrors apps/sync-realtime-data/src/running-style-field-features.ts so the
Python training/evaluation path matches Worker inference.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import numpy as np
import pandas as pd

PACE_NIGE_WEIGHT = 2.0
PACE_SENKOU_WEIGHT = 1.0
PURE_NIGE_THRESHOLD = 0.7
NIGE_CANDIDATE_THRESHOLD = 0.4

PEER_INPUT_COLUMNS: dict[str, str] = {
    "past_nige_rate_self": "pastNigeRate",
    "past_senkou_rate_self": "pastSenkouRate",
    "past_sashi_rate_self": "pastSashiRate",
    "past_oikomi_rate_self": "pastOikomiRate",
    "past_corner_1_norm_avg_5": "pastCorner1NormAvg5",
    "speed_index_avg_5": "speedIndexAvg5",
    "speed_index_best_5": "speedIndexBest5",
    "past_first_3f_avg_5": "pastFirst3fAvg5",
    "kohan_3f_avg_5": "kohan3fAvg5",
    "career_win_rate": "careerWinRate",
}

FIELD_FEATURE_COLUMNS: tuple[str, ...] = (
    "field_nige_pressure",
    "field_senkou_pressure",
    "field_sashi_pressure",
    "field_oikomi_pressure",
    "field_pace_index",
    "field_nige_candidate_count",
    "field_max_past_corner_1_norm",
    "field_min_past_corner_1_norm",
    "field_spread_past_corner_1_norm",
    "field_has_pure_nige_horse",
    "field_avg_speed_index",
    "field_top_speed_index",
    "field_avg_past_first_3f",
    "field_avg_past_kohan_3f",
    "field_avg_career_win_rate",
    "self_nige_rate_minus_field_avg",
    "self_speed_index_vs_field_top",
)


@dataclass(frozen=True)
class HorsePeerInputs:
    past_nige_rate: float | None
    past_senkou_rate: float | None
    past_sashi_rate: float | None
    past_oikomi_rate: float | None
    past_corner_1_norm_avg_5: float | None
    speed_index_avg_5: float | None
    speed_index_best_5: float | None
    past_first_3f_avg_5: float | None
    kohan_3f_avg_5: float | None
    career_win_rate: float | None


def _sum_excluding(values: list[float | None], self_index: int) -> float | None:
    filtered = [value for index, value in enumerate(values) if value is not None and index != self_index]
    if not filtered:
        return None
    return float(sum(filtered))


def _average_excluding(values: list[float | None], self_index: int) -> float | None:
    filtered = [value for index, value in enumerate(values) if value is not None and index != self_index]
    if not filtered:
        return None
    return float(sum(filtered) / len(filtered))


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def compute_field_features_per_horse(horses: list[HorsePeerInputs]) -> list[dict[str, float | int | None]]:
    nige_rates = [horse.past_nige_rate for horse in horses]
    senkou_rates = [horse.past_senkou_rate for horse in horses]
    sashi_rates = [horse.past_sashi_rate for horse in horses]
    oikomi_rates = [horse.past_oikomi_rate for horse in horses]
    speed_avg = [horse.speed_index_avg_5 for horse in horses]
    speed_best = [horse.speed_index_best_5 for horse in horses]
    first_3f = [horse.past_first_3f_avg_5 for horse in horses]
    kohan_3f = [horse.kohan_3f_avg_5 for horse in horses]
    career_win = [horse.career_win_rate for horse in horses]

    rows: list[dict[str, float | int | None]] = []
    for self_index, horse in enumerate(horses):
        peer_nige_rates = [
            value for index, value in enumerate(nige_rates) if index != self_index and value is not None
        ]
        corner1_norm_peers: list[float] = [
            value
            for index in range(len(horses))
            if index != self_index
            for value in [horses[index].past_corner_1_norm_avg_5]
            if value is not None
        ]
        field_nige = _sum_excluding(nige_rates, self_index)
        field_senkou = _sum_excluding(senkou_rates, self_index)
        field_sashi = _sum_excluding(sashi_rates, self_index)
        field_oikomi = _sum_excluding(oikomi_rates, self_index)
        field_pace_index = (
            None
            if field_nige is None or field_senkou is None
            else field_nige * PACE_NIGE_WEIGHT + field_senkou * PACE_SENKOU_WEIGHT
        )
        max_corner = max(corner1_norm_peers) if corner1_norm_peers else None
        min_corner = min(corner1_norm_peers) if corner1_norm_peers else None
        speed_best_non_null = [value for value in speed_best if value is not None]
        self_nige_minus_avg = (
            None
            if horse.past_nige_rate is None or field_nige is None or len(horses) <= 1
            else horse.past_nige_rate - field_nige / (len(horses) - 1)
        )
        rows.append(
            {
                "field_avg_career_win_rate": _average_excluding(career_win, self_index),
                "field_avg_past_first_3f": _average_excluding(first_3f, self_index),
                "field_avg_past_kohan_3f": _average_excluding(kohan_3f, self_index),
                "field_avg_speed_index": _average_excluding(speed_avg, self_index),
                "field_has_pure_nige_horse": int(
                    sum(1 for value in peer_nige_rates if value > PURE_NIGE_THRESHOLD) > 0
                ),
                "field_max_past_corner_1_norm": max_corner,
                "field_min_past_corner_1_norm": min_corner,
                "field_nige_candidate_count": sum(
                    1 for value in peer_nige_rates if value > NIGE_CANDIDATE_THRESHOLD
                ),
                "field_nige_pressure": field_nige,
                "field_oikomi_pressure": field_oikomi,
                "field_pace_index": field_pace_index,
                "field_sashi_pressure": field_sashi,
                "field_senkou_pressure": field_senkou,
                "field_spread_past_corner_1_norm": (
                    None if max_corner is None or min_corner is None else max_corner - min_corner
                ),
                "field_top_speed_index": max(speed_best_non_null) if speed_best_non_null else None,
                "self_nige_rate_minus_field_avg": self_nige_minus_avg,
                "self_speed_index_vs_field_top": _safe_divide(
                    horse.speed_index_best_5,
                    max(speed_best_non_null) if speed_best_non_null else None,
                ),
            }
        )
    return rows


def _row_to_peer_inputs(row: pd.Series) -> HorsePeerInputs:
    return HorsePeerInputs(
        past_nige_rate=_nullable_float(row.get("past_nige_rate_self")),
        past_senkou_rate=_nullable_float(row.get("past_senkou_rate_self")),
        past_sashi_rate=_nullable_float(row.get("past_sashi_rate_self")),
        past_oikomi_rate=_nullable_float(row.get("past_oikomi_rate_self")),
        past_corner_1_norm_avg_5=_nullable_float(row.get("past_corner_1_norm_avg_5")),
        speed_index_avg_5=_nullable_float(row.get("speed_index_avg_5")),
        speed_index_best_5=_nullable_float(row.get("speed_index_best_5")),
        past_first_3f_avg_5=_nullable_float(row.get("past_first_3f_avg_5")),
        kohan_3f_avg_5=_nullable_float(row.get("kohan_3f_avg_5")),
        career_win_rate=_nullable_float(row.get("career_win_rate")),
    )


def _nullable_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if pd.isna(cast(float, value)):
        return None
    return float(cast(float, value))


def enrich_dataframe_with_field_features(df: pd.DataFrame) -> pd.DataFrame:
    """Attach race-internal field features to every row in a feature dataframe."""
    if "race_id" not in df.columns:
        raise ValueError("dataframe must include race_id for field feature enrichment")

    enriched_parts: list[pd.DataFrame] = []
    for _, race_df in df.groupby("race_id", sort=False):
        race_rows = race_df.copy()
        peer_inputs = [_row_to_peer_inputs(row) for _, row in race_rows.iterrows()]
        field_rows = compute_field_features_per_horse(peer_inputs)
        for column in FIELD_FEATURE_COLUMNS:
            race_rows[column] = pd.to_numeric(
                [field_row[column] for field_row in field_rows],
                errors="coerce",
            )
        enriched_parts.append(race_rows)
    return pd.concat(enriched_parts, ignore_index=True)
