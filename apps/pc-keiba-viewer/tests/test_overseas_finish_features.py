from __future__ import annotations

import numpy as np
import pandas as pd

import overseas_finish_features as subject


def _equal_trip_runner(
    *,
    umaban: int,
    kishu_code: str,
    chokyoshi_code: str,
    ketto_toroku_bango: str,
) -> dict[str, object]:
    return {
        "umaban": umaban,
        "weight_raw": "595",
        "age_raw": "05",
        "sex": "1",
        "kishu_code": kishu_code,
        "chokyoshi_code": chokyoshi_code,
        "ketto_toroku_bango": ketto_toroku_bango,
        "kyori": "1600",
        "track_code": "10",
        "grade": "A",
        "field_size_raw": "10",
    }


def _jacques_le_marois_identity_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=1,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=2,
                kishu_code="05621",
                chokyoshi_code="05764",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=3,
                kishu_code="00666",
                chokyoshi_code="01162",
                ketto_toroku_bango="2021105724",
            ),
            _equal_trip_runner(
                umaban=4,
                kishu_code="05509",
                chokyoshi_code="01147",
                ketto_toroku_bango="2021105744",
            ),
            _equal_trip_runner(
                umaban=5,
                kishu_code="05626",
                chokyoshi_code="05665",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=6,
                kishu_code="05464",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=7,
                kishu_code="05659",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=8,
                kishu_code="05504",
                chokyoshi_code="05701",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=9,
                kishu_code="05271",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=10,
                kishu_code="05366",
                chokyoshi_code="05518",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )


def test_feature_columns_exclude_identity_completeness_flags() -> None:
    assert "has_jockey" not in subject.FEATURE_COLUMNS
    assert "has_trainer" not in subject.FEATURE_COLUMNS
    assert "has_horse_reg" not in subject.FEATURE_COLUMNS
    assert subject.FEATURE_COLUMNS == (
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


def test_identity_completeness_flags_mark_only_the_japanese_pair() -> None:
    flags = subject.identity_completeness_flags(_jacques_le_marois_identity_fixture())
    complete = flags["has_jockey"] + flags["has_trainer"] + flags["has_horse_reg"]
    assert complete.tolist() == [0, 2, 3, 3, 2, 1, 1, 2, 1, 2]
    assert flags["has_horse_reg"].tolist() == [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]


def test_engineer_features_identical_when_only_catalogue_codes_differ() -> None:
    japanese = _equal_trip_runner(
        umaban=3,
        kishu_code="00666",
        chokyoshi_code="01162",
        ketto_toroku_bango="2021105724",
    )
    foreign = _equal_trip_runner(
        umaban=5,
        kishu_code="00000",
        chokyoshi_code="00000",
        ketto_toroku_bango="0000000000",
    )
    features = subject.engineer_features(pd.DataFrame([japanese, foreign]))
    assert features.columns.tolist() == list(subject.FEATURE_COLUMNS)
    assert features.iloc[0].tolist() == features.iloc[1].tolist()


def test_score_overseas_card_does_not_rank_japanese_id_pair_first() -> None:
    ranked = subject.score_overseas_card(_jacques_le_marois_identity_fixture())
    top_two = ranked.loc[ranked["predicted_rank"] <= 2, "umaban"].tolist()
    assert top_two != [3, 4]
    assert set(top_two) != {3, 4}
    assert ranked["predicted_rank"].tolist() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert ranked["umaban"].tolist() == [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]


def test_score_overseas_card_uses_regressor_on_identity_free_features() -> None:
    class _HeavierIsWorse:
        def predict(self, features: pd.DataFrame) -> np.ndarray:
            return features["weight_kg"].to_numpy(dtype=float)

    card = pd.DataFrame(
        [
            {
                "umaban": 3,
                "weight_raw": "595",
                "age_raw": "05",
                "sex": "1",
                "kishu_code": "00666",
                "chokyoshi_code": "01162",
                "ketto_toroku_bango": "2021105724",
                "kyori": "1600",
                "track_code": "10",
                "grade": "A",
                "field_size_raw": "02",
            },
            {
                "umaban": 10,
                "weight_raw": "550",
                "age_raw": "03",
                "sex": "2",
                "kishu_code": "00000",
                "chokyoshi_code": "00000",
                "ketto_toroku_bango": "0000000000",
                "kyori": "1600",
                "track_code": "10",
                "grade": "A",
                "field_size_raw": "02",
            },
        ]
    )
    ranked = subject.score_overseas_card(card, regressor=_HeavierIsWorse())
    assert ranked["umaban"].tolist() == [10, 3]
    assert ranked["predicted_rank"].tolist() == [1, 2]
    assert ranked["ketto_toroku_bango"].tolist() == ["UMABAN_10", "2021105724"]


def test_engineer_features_maps_unknown_grade_to_four() -> None:
    row = _equal_trip_runner(
        umaban=1,
        kishu_code="00000",
        chokyoshi_code="00000",
        ketto_toroku_bango="0000000000",
    )
    row["grade"] = "Z"
    features = subject.engineer_features(pd.DataFrame([row]))
    assert features["grade_code"].tolist() == [4]


def test_class_probability_uses_second_column_when_present() -> None:
    class _Proba:
        def predict_proba(self, features: pd.DataFrame) -> np.ndarray:
            return np.array([[0.2, 0.8]] * len(features), dtype=float)

    ranked = subject.score_overseas_card(
        pd.DataFrame(
            [
                _equal_trip_runner(
                    umaban=1,
                    kishu_code="00000",
                    chokyoshi_code="00000",
                    ketto_toroku_bango="0000000000",
                )
            ]
        ),
        clf_top1=_Proba(),
        clf_top3=_Proba(),
    )
    assert ranked["predicted_top1_prob"].tolist() == [0.8]
    assert ranked["predicted_top3_prob"].tolist() == [0.8]


def test_class_probability_is_zero_when_model_returns_one_column() -> None:
    class _Single:
        def predict_proba(self, features: pd.DataFrame) -> np.ndarray:
            return np.array([[1.0]] * len(features), dtype=float)

    ranked = subject.score_overseas_card(
        pd.DataFrame(
            [
                _equal_trip_runner(
                    umaban=2,
                    kishu_code="00000",
                    chokyoshi_code="00000",
                    ketto_toroku_bango="0000000000",
                )
            ]
        ),
        clf_top1=_Single(),
    )
    assert ranked["predicted_top1_prob"].tolist() == [0.0]


def test_partial_odds_do_not_create_a_japan_only_market_signal() -> None:
    card = _jacques_le_marois_identity_fixture()
    card["tansho_odds"] = [
        None,
        None,
        4.6,
        13.4,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    features = subject.engineer_features(card)
    assert features["market_available"].tolist() == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert features["log_odds"].tolist() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]


def test_complete_market_ranks_the_favorite_first_without_a_fitted_model() -> None:
    card = _jacques_le_marois_identity_fixture()
    card["tansho_odds"] = [9.1, 21.1, 4.6, 13.4, 4.4, 60.8, 57.8, 6.4, 9.7, 3.5]
    card["tansho_ninkijun"] = [5, 8, 3, 7, 2, 10, 9, 4, 6, 1]
    ranked = subject.score_overseas_card(card)
    assert ranked["umaban"].tolist()[0] == 10
    assert ranked.loc[ranked["umaban"] == 3, "predicted_rank"].tolist() == [3]
    assert ranked.loc[ranked["umaban"] == 4, "predicted_rank"].tolist() == [7]
    assert set(ranked.loc[ranked["predicted_rank"] <= 2, "umaban"].tolist()) == {10, 5}


def test_attach_tansho_odds_without_ninki_leaves_ninkijun_absent() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=10,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=5,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    attached = subject.attach_tansho_odds(card, odds_by_umaban={10: 3.5, 5: 4.4})
    assert attached["tansho_odds"].tolist() == [3.5, 4.4]
    assert "tansho_ninkijun" not in attached.columns


def test_attach_tansho_odds_writes_live_prices() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=10,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=5,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    attached = subject.attach_tansho_odds(
        card,
        odds_by_umaban={10: 3.5, 5: 4.4},
        ninki_by_umaban={10: 1, 5: 2},
    )
    assert attached["tansho_odds"].tolist() == [3.5, 4.4]
    assert attached["tansho_ninkijun"].tolist() == [1, 2]


def test_inverted_score_model_negates_ranker_output() -> None:
    class _HigherIsBetter:
        def predict(self, features: pd.DataFrame) -> np.ndarray:
            return np.array([2.0, 5.0], dtype=float)

    inverted = subject.InvertedScoreModel(_HigherIsBetter())
    assert inverted.predict(pd.DataFrame({"x": [1, 2]})).tolist() == [-2.0, -5.0]


def test_market_is_complete_rejects_empty_and_submin_odds() -> None:
    assert subject.market_is_complete(pd.Series(dtype=float)) is False
    assert subject.market_is_complete(pd.Series([1.0, 3.5])) is False
    assert subject.market_is_complete(pd.Series([3.5, 4.4])) is True


def test_ninki_falls_back_to_odds_rank_when_ninkijun_absent() -> None:
    card = _jacques_le_marois_identity_fixture()
    card["tansho_odds"] = [9.1, 21.1, 4.6, 13.4, 4.4, 60.8, 57.8, 6.4, 9.7, 3.5]
    features = subject.engineer_features(card)
    assert features["ninki"].tolist() == [5.0, 8.0, 3.0, 7.0, 2.0, 10.0, 9.0, 4.0, 6.0, 1.0]


def test_ninki_fills_missing_ninkijun_from_odds_rank() -> None:
    card = _jacques_le_marois_identity_fixture()
    card["tansho_odds"] = [9.1, 21.1, 4.6, 13.4, 4.4, 60.8, 57.8, 6.4, 9.7, 3.5]
    card["tansho_ninkijun"] = [5, None, 3, 7, 2, 10, 9, 4, 6, 1]
    features = subject.engineer_features(card)
    assert features["ninki"].tolist() == [5.0, 8.0, 3.0, 7.0, 2.0, 10.0, 9.0, 4.0, 6.0, 1.0]


def test_predicted_finish_from_features_is_empty_for_empty_frame() -> None:
    features = pd.DataFrame(
        {
            "weight_kg": [],
            "age": [],
            "sex": [],
            "distance": [],
            "track_code": [],
            "grade_code": [],
            "field_size": [],
            "weight_per_field": [],
            "weight_vs_field": [],
            "age_vs_field": [],
            "is_three_year_old": [],
            "is_female": [],
            "weight_rank": [],
            "age_rank": [],
            "market_available": [],
            "log_odds": [],
            "implied_prob": [],
            "ninki": [],
        }
    )
    assert subject.predicted_finish_from_features(features).tolist() == []


def test_predicted_finish_from_features_uses_log_odds_when_market_complete() -> None:
    features = pd.DataFrame(
        {
            "weight_kg": [59.5, 59.5],
            "age": [5, 5],
            "sex": [1, 1],
            "distance": [1600, 1600],
            "track_code": [10, 10],
            "grade_code": [1, 1],
            "field_size": [2, 2],
            "weight_per_field": [29.75, 29.75],
            "weight_vs_field": [0.0, 0.0],
            "age_vs_field": [0.0, 0.0],
            "is_three_year_old": [0, 0],
            "is_female": [0, 0],
            "weight_rank": [1.5, 1.5],
            "age_rank": [1.5, 1.5],
            "market_available": [1.0, 1.0],
            "log_odds": [1.16, 2.71],
            "implied_prob": [0.31, 0.07],
            "ninki": [1.0, 2.0],
        }
    )
    assert subject.predicted_finish_from_features(features).tolist() == [1.16, 2.71]


def test_predicted_finish_from_features_is_zero_when_market_off() -> None:
    features = pd.DataFrame(
        {
            "weight_kg": [59.5],
            "age": [5],
            "sex": [1],
            "distance": [1600],
            "track_code": [10],
            "grade_code": [1],
            "field_size": [1],
            "weight_per_field": [59.5],
            "weight_vs_field": [0.0],
            "age_vs_field": [0.0],
            "is_three_year_old": [0],
            "is_female": [0],
            "weight_rank": [1.0],
            "age_rank": [1.0],
            "market_available": [0.0],
            "log_odds": [0.0],
            "implied_prob": [0.0],
            "ninki": [0.0],
        }
    )
    assert subject.predicted_finish_from_features(features).tolist() == [0.0]


def test_normalize_positive_scores_keeps_close_rates_close() -> None:
    probs = subject.normalize_positive_scores(
        np.array([0.23, 0.25]),
        missing_prior=0.23,
    )
    assert [round(float(value), 6) for value in probs.tolist()] == [0.479167, 0.520833]


def test_normalize_positive_scores_empty_and_nonpositive_are_uniform() -> None:
    assert subject.normalize_positive_scores(np.array([]), missing_prior=0.23).tolist() == []
    assert subject.normalize_positive_scores(
        np.array([np.nan, np.nan]),
        missing_prior=0.0,
    ).tolist() == [0.5, 0.5]


def test_plackett_luce_from_cost_empty_and_all_nan_are_uniform() -> None:
    assert subject.plackett_luce_from_cost(np.array([]), temperature=2.0).tolist() == []
    assert subject.plackett_luce_from_cost(
        np.array([np.nan, np.nan]),
        temperature=2.0,
    ).tolist() == [0.5, 0.5]


def test_plackett_luce_does_not_force_two_horse_split_to_88_12() -> None:
    probs = subject.plackett_luce_from_cost(np.array([5.4, 5.6]), temperature=2.0)
    assert [round(float(value), 6) for value in probs.tolist()] == [0.524979, 0.475021]


def test_summarize_netkeiba_form_returns_empty_for_empty_history() -> None:
    summary = subject.summarize_netkeiba_form(
        pd.DataFrame(),
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 0
    assert summary.wins == 0
    assert summary.avg_finish is None
    assert summary.last_finish is None
    assert summary.last_distance_delta is None
    assert summary.days_since is None


def test_summarize_netkeiba_form_ignores_jra_van_as_extra_starts() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 2,
                "distance_metres": 1600,
            },
            {
                "source": "jra-van",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 2,
                "distance_metres": 1600,
            },
            {
                "source": "jra-van",
                "source_horse_id": "horse-a",
                "race_date": "2026-05-01",
                "finish_position": 1,
                "distance_metres": 1400,
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 1
    assert summary.wins == 0
    assert summary.avg_finish == 2.0
    assert summary.last_finish == 2.0
    assert summary.last_distance_delta == 0
    assert summary.days_since == 46


def test_summarize_netkeiba_form_excludes_same_day_future_and_other_horses() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-08-16",
                "finish_position": 1,
                "distance_metres": 1600,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-08-17",
                "finish_position": 1,
                "distance_metres": 1600,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-b",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "distance_metres": 1600,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-06-01",
                "finish_position": 4,
                "distance_metres": 1400,
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="20260816",
        target_distance=1600,
    )
    assert summary.starts == 1
    assert summary.wins == 0
    assert summary.avg_finish == 4.0
    assert summary.last_finish == 4.0
    assert summary.last_distance_delta == -200
    assert summary.days_since == 76


def test_summarize_netkeiba_form_counts_wins_and_uses_latest_start() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-05-01",
                "finish_position": 1,
                "distance_metres": 2000,
                "race_day_sequence": 1,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-10",
                "finish_position": 3,
                "distance_metres": 1800,
                "race_day_sequence": 2,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-10",
                "finish_position": 1,
                "distance_metres": 1400,
                "race_day_sequence": 4,
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 3
    assert summary.wins == 2
    assert summary.avg_finish == 1.6666666666666667
    assert summary.last_finish == 1.0
    assert summary.last_distance_delta == -200
    assert summary.days_since == 37
    assert summary.recent_avg_finish == 1.6666666666666667


def test_summarize_netkeiba_form_recent_avg_uses_last_three_only() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2025-01-01",
                "finish_position": 12,
                "distance_metres": 1600,
                "race_day_sequence": 1,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-05-01",
                "finish_position": 2,
                "distance_metres": 1600,
                "race_day_sequence": 1,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-06-01",
                "finish_position": 1,
                "distance_metres": 1600,
                "race_day_sequence": 1,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 3,
                "distance_metres": 1600,
                "race_day_sequence": 1,
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 4
    assert summary.avg_finish == 4.5
    assert summary.last_finish == 3.0
    assert summary.recent_avg_finish == 2.0


def test_summarize_netkeiba_form_empty_when_all_rows_are_on_or_after_race_date() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-08-16",
                "finish_position": 1,
                "distance_metres": 1600,
            }
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 0
    assert summary.avg_finish is None


def test_summarize_netkeiba_form_returns_empty_when_source_column_missing() -> None:
    history = pd.DataFrame(
        [
            {
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "distance_metres": 1600,
            }
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 0
    assert summary.avg_finish is None


def test_summarize_netkeiba_form_returns_empty_for_invalid_calendar_date() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "distance_metres": 1600,
            }
        ]
    )
    month_overflow = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="20261301",
        target_distance=1600,
    )
    day_overflow = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="20260800",
        target_distance=1600,
    )
    february_overflow = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="20260230",
        target_distance=1600,
    )
    assert month_overflow.starts == 0
    assert day_overflow.starts == 0
    assert february_overflow.starts == 0


def test_as_timestamp_rejects_nat() -> None:
    assert subject._as_timestamp(pd.NaT) is None


def test_summarize_netkeiba_form_skips_nat_history_dates() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": pd.Timestamp("NaT"),
                "finish_position": 1,
                "distance_metres": 1600,
            }
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 0
    assert summary.avg_finish is None


def test_summarize_netkeiba_form_returns_empty_for_unparseable_race_date() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "distance_metres": 1600,
            }
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="not-a-date",
        target_distance=1600,
    )
    assert summary.starts == 0
    assert summary.days_since is None


def test_attach_netkeiba_form_writes_summary_columns_and_empty_default() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=1,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=2,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    attached = subject.attach_netkeiba_form(
        card,
        form_by_umaban={
            1: subject.FormSummary(
                starts=4,
                wins=2,
                avg_finish=2.5,
                last_finish=1.0,
                last_distance_delta=-200,
                days_since=14,
            )
        },
    )
    assert attached["form_starts"].tolist() == [4, 0]
    assert attached["form_wins"].tolist() == [2, 0]
    assert attached["form_avg_finish"].tolist()[0] == 2.5
    assert attached["form_last_finish"].tolist()[0] == 1.0
    assert attached["form_last_distance_delta"].tolist()[0] == -200
    assert attached["form_days_since"].tolist()[0] == 14
    assert attached["form_starts"].tolist()[1] == 0
    assert bool(pd.isna(attached["form_avg_finish"].iloc[1])) is True


def test_form_prior_score_blends_avg_and_last_finish() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame({"form_avg_finish": [5.0], "form_last_finish": [2.0]})
    )
    assert [round(float(scores[0]), 10)] == [4.0]


def test_form_prior_score_weights_recent_ahead_of_career_average() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_recent_avg_finish": [2.0],
                "form_last_finish": [1.0],
                "form_avg_finish": [8.0],
            }
        )
    )
    assert scores.tolist() == [4.2]


def test_form_prior_score_uses_avg_when_last_missing() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame({"form_avg_finish": [3.0], "form_last_finish": [None]})
    )
    assert scores.tolist() == [3.0]


def test_form_prior_score_uses_last_when_avg_missing() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame({"form_avg_finish": [None], "form_last_finish": [5.0]})
    )
    assert scores.tolist() == [5.0]


def test_form_prior_score_fills_missing_with_shared_field_mean() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_avg_finish": [2.0, None, None],
                "form_last_finish": [2.0, None, None],
                "field_size_raw": ["10", "10", "10"],
                "ketto_toroku_bango": ["2021105724", "0000000000", "2021105744"],
            }
        )
    )
    assert scores.tolist() == [2.0, 5.5, 5.5]


def test_form_prior_score_all_missing_uses_same_neutral_not_japan_default() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_avg_finish": [None, None],
                "form_last_finish": [None, None],
                "field_size_raw": ["10", "10"],
                "ketto_toroku_bango": ["2021105724", "0000000000"],
            }
        )
    )
    assert scores.tolist() == [5.5, 5.5]


def test_form_prior_score_empty_frame_is_empty() -> None:
    assert subject.form_prior_score(pd.DataFrame()).tolist() == []


def test_form_prior_score_missing_columns_without_field_size_is_zero() -> None:
    assert subject.form_prior_score(pd.DataFrame({"umaban": [1, 2]})).tolist() == [0.0, 0.0]


def test_form_prior_score_ignores_non_numeric_field_size() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_avg_finish": [2.0, None, 6.0],
                "form_last_finish": [2.0, None, 6.0],
                "field_size_raw": [None, None, None],
            }
        )
    )
    assert scores.tolist() == [2.0, 4.0, 6.0]


def test_form_prior_score_uses_known_mean_when_field_size_absent() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_avg_finish": [2.0, None, 6.0],
                "form_last_finish": [2.0, None, 6.0],
            }
        )
    )
    assert scores.tolist() == [2.0, 4.0, 6.0]


def test_blend_weights_when_market_is_complete() -> None:
    blended = subject.blend_predicted_finish(
        model_finish=np.array([3.0, 3.0]),
        form_finish=np.array([1.0, 8.0]),
        market_finish=np.array([8.0, 1.0]),
        market_available=True,
        market_odds=np.array([15.2, 3.2]),
    )
    assert blended[0] < blended[1]
    assert subject.BLEND_MARKET_WEIGHT == 0.1


def test_blend_weights_when_market_is_off() -> None:
    blended = subject.blend_predicted_finish(
        model_finish=np.array([8.0, 1.0]),
        form_finish=np.array([1.0, 8.0]),
        market_finish=np.array([1.0, 2.0]),
        market_available=False,
    )
    assert blended[0] < blended[1]


def test_complete_market_does_not_copy_ninki_when_form_disagrees() -> None:
    card = pd.DataFrame(
        [
            {
                **_equal_trip_runner(
                    umaban=10,
                    kishu_code="00000",
                    chokyoshi_code="00000",
                    ketto_toroku_bango="0000000000",
                ),
                "tansho_odds": 3.2,
                "tansho_ninkijun": 1,
            },
            {
                **_equal_trip_runner(
                    umaban=1,
                    kishu_code="00000",
                    chokyoshi_code="00000",
                    ketto_toroku_bango="0000000000",
                ),
                "tansho_odds": 15.2,
                "tansho_ninkijun": 6,
            },
        ]
    )
    ranked = subject.score_overseas_card(
        card,
        form_by_umaban={
            10: subject.FormSummary(
                starts=6,
                wins=1,
                avg_finish=7.0,
                last_finish=8.0,
                last_distance_delta=0,
                days_since=20,
                recent_avg_finish=7.5,
            ),
            1: subject.FormSummary(
                starts=6,
                wins=4,
                avg_finish=2.0,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=14,
                recent_avg_finish=1.5,
            ),
        },
    )
    assert ranked["umaban"].tolist() == [1, 10]
    assert ranked["predicted_rank"].tolist() == [1, 2]


def test_blend_uses_model_only_when_form_is_uniform_and_market_off() -> None:
    blended = subject.blend_predicted_finish(
        model_finish=np.array([2.0, 4.0]),
        form_finish=np.array([3.0, 3.0]),
        market_finish=np.array([0.0, 0.0]),
        market_available=False,
    )
    assert blended[0] < blended[1]


def test_blend_predicted_finish_empty_arrays_return_empty() -> None:
    blended = subject.blend_predicted_finish(
        model_finish=np.array([]),
        form_finish=np.array([]),
        market_finish=np.array([]),
        market_available=True,
    )
    assert blended.tolist() == []


def test_empty_history_keeps_model_only_ranks() -> None:
    card = _jacques_le_marois_identity_fixture()
    ranked_model = subject.score_overseas_card(card)
    ranked_empty = subject.score_overseas_card(card, form_by_umaban={})
    assert ranked_model["umaban"].tolist() == [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    assert ranked_empty["umaban"].tolist() == [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    assert ranked_empty["predicted_rank"].tolist() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_better_netkeiba_form_ranks_ahead_when_model_tied_and_market_off() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=8,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=2,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    ranked = subject.score_overseas_card(
        card,
        form_by_umaban={
            8: subject.FormSummary(
                starts=3,
                wins=0,
                avg_finish=7.0,
                last_finish=8.0,
                last_distance_delta=0,
                days_since=21,
            ),
            2: subject.FormSummary(
                starts=3,
                wins=2,
                avg_finish=1.5,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=14,
            ),
        },
    )
    assert ranked["umaban"].tolist() == [2, 8]
    assert ranked["predicted_rank"].tolist() == [1, 2]


def test_japanese_id_pair_does_not_outrank_better_foreign_form() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=1,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=3,
                kishu_code="00666",
                chokyoshi_code="01162",
                ketto_toroku_bango="2021105724",
            ),
            _equal_trip_runner(
                umaban=4,
                kishu_code="05509",
                chokyoshi_code="01147",
                ketto_toroku_bango="2021105744",
            ),
        ]
    )
    ranked = subject.score_overseas_card(
        card,
        form_by_umaban={
            1: subject.FormSummary(
                starts=5,
                wins=3,
                avg_finish=1.8,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=14,
            ),
            3: subject.FormSummary(
                starts=0,
                wins=0,
                avg_finish=None,
                last_finish=None,
                last_distance_delta=None,
                days_since=None,
            ),
            4: subject.FormSummary(
                starts=0,
                wins=0,
                avg_finish=None,
                last_finish=None,
                last_distance_delta=None,
                days_since=None,
            ),
        },
    )
    assert ranked["umaban"].tolist() == [1, 4, 3]
    assert ranked["predicted_rank"].tolist() == [1, 2, 3]
    assert ranked["ketto_toroku_bango"].tolist() == ["UMABAN_01", "2021105744", "2021105724"]


def test_summarize_netkeiba_form_handles_timestamp_dates_and_missing_result_columns() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": pd.Timestamp("2026-07-01"),
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "not-a-date",
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026/08/16",
        target_distance=1600,
    )
    assert summary.starts == 1
    assert summary.wins == 0
    assert summary.avg_finish is None
    assert summary.last_finish is None
    assert summary.last_distance_delta is None
    assert summary.days_since == 46


def test_summarize_netkeiba_form_last_finish_can_be_missing_on_latest_start() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-10",
                "finish_position": None,
                "distance_metres": None,
            },
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-05-01",
                "finish_position": 2,
                "distance_metres": 1800,
            },
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.starts == 2
    assert summary.wins == 0
    assert summary.avg_finish == 2.0
    assert summary.last_finish is None
    assert summary.last_distance_delta is None
    assert summary.days_since == 37


def test_form_prior_score_uses_field_size_column_when_raw_absent() -> None:
    scores = subject.form_prior_score(
        pd.DataFrame(
            {
                "form_avg_finish": [None],
                "form_last_finish": [None],
                "field_size": [8],
            }
        )
    )
    assert scores.tolist() == [4.5]


def test_attach_netkeiba_form_treats_non_numeric_umaban_as_empty() -> None:
    attached = subject.attach_netkeiba_form(
        pd.DataFrame({"umaban": ["x"]}),
        form_by_umaban={
            1: subject.FormSummary(
                starts=2,
                wins=1,
                avg_finish=1.0,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=10,
            )
        },
    )
    assert attached["form_starts"].tolist() == [0]
    assert bool(pd.isna(attached["form_avg_finish"].iloc[0])) is True


def test_blend_uses_model_when_form_is_all_nan_and_market_off() -> None:
    blended = subject.blend_predicted_finish(
        model_finish=np.array([2.0, 4.0]),
        form_finish=np.array([np.nan, np.nan]),
        market_finish=np.array([0.0, 0.0]),
        market_available=False,
    )
    assert blended[0] < blended[1]


def test_japanese_id_pair_still_does_not_win_with_empty_form_lookup() -> None:
    ranked = subject.score_overseas_card(
        _jacques_le_marois_identity_fixture(),
        form_by_umaban={},
    )
    top_two = ranked.loc[ranked["predicted_rank"] <= 2, "umaban"].tolist()
    assert top_two != [3, 4]
    assert set(top_two) != {3, 4}
    assert ranked["umaban"].tolist() == [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]


def test_parse_race_grade_reads_g1_g2_g3_and_plain() -> None:
    assert subject.parse_race_grade("ロートシルト賞(GI)") == 1
    assert subject.parse_race_grade("サマーマイルステーク(GII)") == 2
    assert subject.parse_race_grade("バーデナーマイレ(GIII)") == 3
    assert subject.parse_race_grade("条件戦") == 0


def test_prize_points_pay_g1_win_more_than_condition_win() -> None:
    assert subject.prize_points_for_start(finish_position=1, race_name="安田記念(GI)") == 100.0
    assert subject.prize_points_for_start(finish_position=1, race_name="読売マイラーズC(GII)") == 40.0
    assert subject.prize_points_for_start(finish_position=1, race_name="バーデナーマイレ(GIII)") == 20.0
    assert subject.prize_points_for_start(finish_position=1, race_name="条件戦") == 8.0
    assert subject.prize_points_for_start(finish_position=2, race_name="ロートシルト賞(GI)") == 38.0
    assert subject.prize_points_for_start(finish_position=3, race_name="クイーンアンS(GI)") == 22.0
    assert subject.prize_points_for_start(finish_position=4, race_name="条件戦") == 0.96
    assert subject.prize_points_for_start(finish_position=8, race_name="条件戦") == 0.24


def test_summarize_netkeiba_form_adds_prize_per_start_for_g1_win() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "source_horse_id": "horse-a",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "race_name": "コロネーションS(GI)",
                "distance_metres": 1600,
                "race_day_sequence": 1,
            }
        ]
    )
    summary = subject.summarize_netkeiba_form(
        history,
        source_horse_id="horse-a",
        race_date="2026-08-16",
        target_distance=1600,
    )
    assert summary.prize_points == 100.0
    assert summary.prize_per_start == 100.0
    assert summary.win_rate == 1.0


def test_summarize_netkeiba_person_counts_win_show_and_prize() -> None:
    history = pd.DataFrame(
        [
            {
                "source": "netkeiba",
                "person_kind": "jockey",
                "source_person_id": "05366",
                "race_date": "2026-07-01",
                "finish_position": 1,
                "race_name": "ファルマスS(GI)",
            },
            {
                "source": "netkeiba",
                "person_kind": "jockey",
                "source_person_id": "05366",
                "race_date": "2026-06-01",
                "finish_position": 3,
                "race_name": "条件戦",
            },
            {
                "source": "jra-van",
                "person_kind": "jockey",
                "source_person_id": "05366",
                "race_date": "2026-05-01",
                "finish_position": 1,
                "race_name": "G1 extra",
            },
        ]
    )
    summary = subject.summarize_netkeiba_person(
        history,
        person_kind="jockey",
        source_person_id="05366",
        race_date="2026-08-16",
    )
    assert summary.starts == 2
    assert summary.wins == 1
    assert summary.shows == 2
    assert summary.win_rate == 0.5
    assert summary.show_rate == 1.0
    assert summary.prize_points == 101.76


def test_g1_prize_outranks_condition_winner_when_finishes_match() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=2,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=8,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    ranked = subject.score_overseas_card(
        card,
        form_by_umaban={
            2: subject.FormSummary(
                starts=2,
                wins=2,
                avg_finish=1.0,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=10,
                recent_avg_finish=1.0,
                prize_points=16.0,
                prize_per_start=8.0,
                win_rate=1.0,
            ),
            8: subject.FormSummary(
                starts=2,
                wins=2,
                avg_finish=1.0,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=10,
                recent_avg_finish=1.0,
                prize_points=200.0,
                prize_per_start=100.0,
                win_rate=1.0,
            ),
        },
    )
    assert ranked["umaban"].tolist() == [8, 2]
    assert ranked["predicted_rank"].tolist() == [1, 2]


def test_better_jockey_outranks_when_horse_form_is_tied() -> None:
    card = pd.DataFrame(
        [
            _equal_trip_runner(
                umaban=2,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
            _equal_trip_runner(
                umaban=8,
                kishu_code="00000",
                chokyoshi_code="00000",
                ketto_toroku_bango="0000000000",
            ),
        ]
    )
    ranked = subject.score_overseas_card(
        card,
        form_by_umaban={
            2: subject.FormSummary(
                starts=3,
                wins=1,
                avg_finish=3.0,
                last_finish=3.0,
                last_distance_delta=0,
                days_since=14,
                recent_avg_finish=3.0,
            ),
            8: subject.FormSummary(
                starts=3,
                wins=1,
                avg_finish=3.0,
                last_finish=3.0,
                last_distance_delta=0,
                days_since=14,
                recent_avg_finish=3.0,
            ),
        },
        jockey_by_umaban={
            2: subject.PersonSummary(
                starts=20,
                wins=1,
                shows=3,
                win_rate=0.05,
                show_rate=0.15,
                prize_points=10.0,
                prize_per_start=0.5,
            ),
            8: subject.PersonSummary(
                starts=20,
                wins=10,
                shows=14,
                win_rate=0.5,
                show_rate=0.7,
                prize_points=80.0,
                prize_per_start=4.0,
            ),
        },
    )
    assert ranked["umaban"].tolist() == [8, 2]
    assert ranked["predicted_rank"].tolist() == [1, 2]


def test_summarize_netkeiba_person_empty_when_kind_missing() -> None:
    summary = subject.summarize_netkeiba_person(
        pd.DataFrame({"source": ["netkeiba"], "source_person_id": ["x"], "race_date": ["2026-01-01"]}),
        person_kind="jockey",
        source_person_id="x",
        race_date="2026-08-16",
    )
    assert summary.starts == 0
    assert summary.win_rate is None


def test_summarize_netkeiba_person_empty_for_unparseable_race_date() -> None:
    summary = subject.summarize_netkeiba_person(
        pd.DataFrame(
            {
                "source": ["netkeiba"],
                "person_kind": ["jockey"],
                "source_person_id": ["x"],
                "race_date": ["2026-01-01"],
                "finish_position": [1],
                "race_name": ["条件戦"],
            }
        ),
        person_kind="jockey",
        source_person_id="x",
        race_date="not-a-date",
    )
    assert summary.starts == 0


def test_prize_prior_score_marks_missing_as_nan() -> None:
    scores = subject.prize_prior_score(
        [
            subject.FormSummary(
                starts=1,
                wins=1,
                avg_finish=1.0,
                last_finish=1.0,
                last_distance_delta=0,
                days_since=1,
                prize_per_start=10.0,
            ),
            subject.EMPTY_FORM,
        ]
    )
    assert scores.tolist()[0] == -10.0
    assert bool(np.isnan(scores.tolist()[1])) is True


def test_history_prize_points_empty_frame_is_zero() -> None:
    assert subject._history_prize_points(pd.DataFrame()) == 0.0


def test_summarize_netkeiba_person_empty_when_no_prior_starts() -> None:
    summary = subject.summarize_netkeiba_person(
        pd.DataFrame(
            {
                "source": ["netkeiba"],
                "person_kind": ["jockey"],
                "source_person_id": ["x"],
                "race_date": ["2026-08-16"],
                "finish_position": [1],
                "race_name": ["条件戦"],
            }
        ),
        person_kind="jockey",
        source_person_id="x",
        race_date="2026-08-16",
    )
    assert summary.starts == 0


def test_people_for_non_numeric_umaban_is_empty() -> None:
    summary = subject._people_for_umaban("x", people_by_umaban={1: subject.EMPTY_PERSON})
    assert summary.starts == 0


def test_normalize_overround_removes_track_take() -> None:
    probs = subject.normalize_overround(np.array([2.0, 4.0]))
    assert [round(float(value), 6) for value in probs.tolist()] == [0.666667, 0.333333]


def test_normalize_overround_empty_is_empty() -> None:
    assert subject.normalize_overround(np.array([])).tolist() == []


def test_empirical_bayes_rate_shrinks_small_sample() -> None:
    raw_half = 3.0 / 6.0
    shrunk = subject.empirical_bayes_rate(successes=3.0, trials=6.0, prior_rate=0.2, strength=20.0)
    assert shrunk == 0.2692307692307692
    assert shrunk < raw_half


def test_empirical_bayes_rate_zero_trials_returns_prior() -> None:
    assert subject.empirical_bayes_rate(successes=0.0, trials=0.0, prior_rate=0.15) == 0.15


def test_small_sample_win_rate_does_not_outrank_large_sample() -> None:
    scores = subject.person_prior_score(
        [
            subject.PersonSummary(
                starts=6,
                wins=3,
                shows=4,
                win_rate=0.5,
                show_rate=0.6666666666666666,
                prize_points=30.0,
                prize_per_start=5.0,
            ),
            subject.PersonSummary(
                starts=2015,
                wins=508,
                shows=1052,
                win_rate=0.2521091811414392,
                show_rate=0.5220843672456576,
                prize_points=20000.0,
                prize_per_start=9.925558312655087,
            ),
        ]
    )
    assert scores[1] < scores[0]


def test_engineer_features_implied_prob_sums_to_one() -> None:
    card = _jacques_le_marois_identity_fixture()
    card["tansho_odds"] = [9.1, 21.1, 4.6, 13.4, 4.4, 60.8, 57.8, 6.4, 9.7, 3.5]
    features = subject.engineer_features(card)
    assert round(float(features["implied_prob"].sum()), 10) == 1.0


def test_person_prior_score_uses_show_rate_when_win_rate_missing() -> None:
    scores = subject.person_prior_score(
        [
            subject.PersonSummary(
                starts=10,
                wins=0,
                shows=4,
                win_rate=None,
                show_rate=0.4,
                prize_points=0.0,
                prize_per_start=None,
            )
        ]
    )
    assert [round(float(scores[0]), 6)] == [0.793333]


def test_viewpoint_win_probabilities_each_sum_to_one() -> None:
    components, weights = subject.viewpoint_win_probabilities(
        model_finish=np.array([3.0, 5.0]),
        form_finish=np.array([2.0, 6.0]),
        market_finish=np.array([1.0, 2.0]),
        market_available=True,
        prize_finish=np.array([-10.0, -2.0]),
        jockey_finish=np.array([0.7, 0.8]),
        trainer_finish=np.array([0.75, 0.72]),
        owner_finish=np.array([0.77, 0.76]),
        market_odds=np.array([3.2, 15.2]),
    )
    assert weights[5] == 0.1
    assert [round(float(row.sum()), 10) for row in components] == [
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
    ]


def test_log_opinion_pool_and_finish_are_monotone_in_probability() -> None:
    pooled = subject.log_opinion_pool(
        [np.array([0.7, 0.3]), np.array([0.6, 0.4])],
        [0.5, 0.5],
    )
    assert [round(float(value), 6) for value in pooled.tolist()] == [0.651669, 0.348331]
    finish = -np.log(pooled)
    assert finish[0] < finish[1]


def test_close_person_qualities_do_not_become_an_88_12_split() -> None:
    scores = subject.person_prior_score(
        [
            subject.PersonSummary(2015, 508, 1052, 0.25, 0.52, 20000.0, 9.9),
            subject.PersonSummary(1800, 470, 980, 0.26, 0.54, 18000.0, 10.0),
        ]
    )
    probs = subject.normalize_positive_scores(
        1.0 - scores,
        missing_prior=0.23,
    )
    assert [round(float(value), 6) for value in probs.tolist()] == [0.490408, 0.509592]


def test_softmax_and_none_probability_helpers_cover_empty_inputs() -> None:
    assert subject._softmax(np.array([])).tolist() == []
    assert subject._probs_from_cost(None, size=2, temperature=2.0).tolist() == [0.5, 0.5]
    assert subject._probs_from_positive(None, size=2, missing_prior=0.23).tolist() == [0.5, 0.5]
    assert subject._positive_from_lower_is_better(None) is None
    assert subject._quality_from_person_cost(None) is None


def test_log_opinion_pool_empty_rows_is_empty() -> None:
    assert subject.log_opinion_pool([], []).tolist() == []


def test_is_informative_probs_rejects_empty() -> None:
    assert subject._is_informative_probs(np.array([])) is False


def test_regressor_is_informative_rejects_all_nan() -> None:
    assert subject.regressor_is_informative(np.array([np.nan, np.nan])) is False


def test_plackett_luce_imputes_missing_with_known_mean() -> None:
    probs = subject.plackett_luce_from_cost(np.array([2.0, np.nan]), temperature=2.0)
    assert [round(float(value), 6) for value in probs.tolist()] == [0.5, 0.5]
