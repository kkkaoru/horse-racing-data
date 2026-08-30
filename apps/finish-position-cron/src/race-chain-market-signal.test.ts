// Run with bun. Golden parity and fail-closed tests for the Worker-native
// focused-race market-signal transform.

import { expect, test } from "vitest";

import { materializeRaceMarketSignals } from "./race-chain-market-signal";

test("materializeRaceMarketSignals matches the canonical DuckDB focused-race golden values", () => {
  const result = materializeRaceMarketSignals({
    liveOddsByHorseNumber: new Map(),
    raceId: "jra:2026:0824:05:11",
    rows: [
      {
        career_win_rate: 0.3,
        ketto_toroku_bango: "horse-fav",
        odds_score: 0.5,
        popularity_score: 0,
        race_id: "jra:2026:0824:05:11",
        shusso_tosu_1: 10,
        tansho_ninkijun: 1,
        tansho_odds: 5,
        umaban: 1,
      },
      {
        career_win_rate: 0.2,
        ketto_toroku_bango: "horse-mid",
        odds_score: 0.3,
        popularity_score: 0.5,
        race_id: "jra:2026:0824:05:11",
        shusso_tosu_1: 10,
        tansho_ninkijun: 2,
        tansho_odds: 8,
        umaban: 2,
      },
      {
        career_win_rate: 0.1,
        ketto_toroku_bango: "horse-out",
        odds_score: 0.2,
        popularity_score: 1,
        race_id: "jra:2026:0824:05:11",
        shusso_tosu_1: 10,
        tansho_ninkijun: 3,
        tansho_odds: 20,
        umaban: 3,
      },
    ],
  });

  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("expected ready market-signal result");
  expect(result.rows[0]?.inverse_odds_implied_prob).toBeCloseTo(0.2, 12);
  expect(result.rows[0]?.inverse_odds_market_share).toBeCloseTo(0.5333333333333333, 12);
  expect(result.rows[0]?.inverse_odds_rank_in_race).toBe(1);
  expect(result.rows[0]?.popularity_rank_in_race).toBe(1);
  expect(result.rows[0]?.odds_score_diff_from_race_avg).toBeCloseTo(0.1666666666666667, 12);
  expect(result.rows[0]?.popularity_score_diff_from_race_avg).toBeCloseTo(-0.5, 12);
  expect(result.rows[0]?.popularity_odds_disagreement).toBeCloseTo(0.5, 12);
  expect(result.rows[0]?.form_market_edge).toBeCloseTo(0.1, 12);
  expect(result.rows[0]?.field_dominant_favorite_indicator).toBeCloseTo(0.625, 12);
  expect(result.rows[0]?.horse_popularity_vs_field).toBeCloseTo(0.1, 12);
  expect(result.rows[1]?.inverse_odds_implied_prob).toBeCloseTo(0.125, 12);
  expect(result.rows[1]?.inverse_odds_market_share).toBeCloseTo(0.3333333333333333, 12);
  expect(result.rows[1]?.inverse_odds_rank_in_race).toBe(2);
  expect(result.rows[1]?.popularity_rank_in_race).toBe(2);
  expect(result.rows[1]?.odds_score_diff_from_race_avg).toBeCloseTo(-0.0333333333333333, 12);
  expect(result.rows[1]?.popularity_score_diff_from_race_avg).toBeCloseTo(0, 12);
  expect(result.rows[1]?.popularity_odds_disagreement).toBeCloseTo(0.2, 12);
  expect(result.rows[1]?.form_market_edge).toBeCloseTo(0.075, 12);
  expect(result.rows[1]?.horse_popularity_vs_field).toBeCloseTo(0.2, 12);
  expect(result.rows[2]?.inverse_odds_implied_prob).toBeCloseTo(0.05, 12);
  expect(result.rows[2]?.inverse_odds_market_share).toBeCloseTo(0.1333333333333333, 12);
  expect(result.rows[2]?.inverse_odds_rank_in_race).toBe(3);
  expect(result.rows[2]?.popularity_rank_in_race).toBe(3);
  expect(result.rows[2]?.odds_score_diff_from_race_avg).toBeCloseTo(-0.1333333333333333, 12);
  expect(result.rows[2]?.popularity_score_diff_from_race_avg).toBeCloseTo(0.5, 12);
  expect(result.rows[2]?.popularity_odds_disagreement).toBeCloseTo(0.8, 12);
  expect(result.rows[2]?.form_market_edge).toBeCloseTo(0.05, 12);
  expect(result.rows[2]?.horse_popularity_vs_field).toBeCloseTo(0.3, 12);
});

test("materializeRaceMarketSignals preserves DuckDB competition ties and NULLS LAST ranks", () => {
  const result = materializeRaceMarketSignals({
    liveOddsByHorseNumber: new Map(),
    raceId: "jra:2026:0824:05:12",
    rows: [
      {
        career_win_rate: 0.4,
        odds_score: 0.1,
        popularity_score: 0.2,
        race_id: "jra:2026:0824:05:12",
        tansho_ninkijun: 1,
        tansho_odds: 2,
        umaban: 1,
      },
      {
        career_win_rate: 0.1,
        odds_score: 0.3,
        popularity_score: 0.4,
        race_id: "jra:2026:0824:05:12",
        tansho_ninkijun: 1,
        tansho_odds: 2,
        umaban: 2,
      },
      {
        career_win_rate: null,
        odds_score: null,
        popularity_score: null,
        race_id: "jra:2026:0824:05:12",
        tansho_ninkijun: 3,
        tansho_odds: null,
        umaban: 3,
      },
    ],
  });

  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("expected ready market-signal result");
  expect(result.rows[0]?.inverse_odds_rank_in_race).toBe(1);
  expect(result.rows[1]?.inverse_odds_rank_in_race).toBe(1);
  expect(result.rows[2]?.inverse_odds_rank_in_race).toBe(3);
  expect(result.rows[0]?.popularity_rank_in_race).toBe(1);
  expect(result.rows[1]?.popularity_rank_in_race).toBe(1);
  expect(result.rows[2]?.popularity_rank_in_race).toBe(3);
  expect(result.rows[2]?.tansho_ninkijun_raw).toBeNull();
  expect(result.rows[2]?.inverse_odds_market_share).toBeNull();
  expect(result.rows[2]?.odds_score_diff_from_race_avg).toBeNull();
  expect(result.rows[2]?.popularity_score_diff_from_race_avg).toBeNull();
  expect(result.rows[2]?.popularity_odds_disagreement).toBeNull();
  expect(result.rows[2]?.form_market_edge).toBeNull();
});

test("materializeRaceMarketSignals merges a complete live odds board before deriving signals", () => {
  const result = materializeRaceMarketSignals({
    liveOddsByHorseNumber: new Map([
      [1, { tanshoNinkijun: 1, tanshoOdds: 2 }],
      [2, { tanshoNinkijun: 2, tanshoOdds: 4 }],
      [3, { tanshoNinkijun: 3, tanshoOdds: 8 }],
    ]),
    raceId: "jra:2026:0824:05:11",
    rows: [
      {
        career_win_rate: 0.6,
        odds_score: 0.8,
        popularity_score: 0.8,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: 9,
        tansho_odds: 99,
        umaban: 1,
      },
      {
        career_win_rate: 0.4,
        odds_score: 0.8,
        popularity_score: 0.8,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: 8,
        tansho_odds: 88,
        umaban: 2,
      },
      {
        career_win_rate: 0.2,
        odds_score: 0.8,
        popularity_score: 0.8,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: 7,
        tansho_odds: 77,
        umaban: 3,
      },
    ],
  });

  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("expected ready market-signal result");
  expect(result.rows[0]?.tansho_odds).toBe(2);
  expect(result.rows[0]?.tansho_ninkijun).toBe(1);
  expect(result.rows[0]?.odds_score).toBeCloseTo(0.12152412607595545, 12);
  expect(result.rows[0]?.popularity_score).toBeCloseTo(0, 12);
  expect(result.rows[0]?.inverse_odds_market_share).toBeCloseTo(0.5714285714285714, 12);
  expect(result.rows[0]?.odds_score_diff_from_race_avg).toBeCloseTo(-0.12152412607595545, 12);
  expect(result.rows[1]?.odds_score_diff_from_race_avg).toBeCloseTo(0, 12);
  expect(result.rows[2]?.odds_score_diff_from_race_avg).toBeCloseTo(0.12152412607595545, 12);
  expect(result.rows[2]?.popularity_score).toBeCloseTo(1, 12);
  expect(result.rows[2]?.inverse_odds_market_share).toBeCloseTo(0.14285714285714285, 12);
});

test("materializeRaceMarketSignals returns fallback for an empty foundation", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: [],
    }),
  ).toStrictEqual({ reason: "empty-foundation", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback above the foundation runner bound", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: Array.from({ length: 33 }, (_, index) => ({
        career_win_rate: 0.1,
        odds_score: 0.2,
        popularity_score: 0.3,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: index + 1,
        tansho_odds: index + 2,
        umaban: index + 1,
      })),
    }),
  ).toStrictEqual({ reason: "runner-limit", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback when a row belongs to another race", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:12",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
      ],
    }),
  ).toStrictEqual({ reason: "race-contract-mismatch", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for an invalid horse number", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1.5,
        },
      ],
    }),
  ).toStrictEqual({ reason: "invalid-horse-number", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for duplicate horse numbers", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
        {
          career_win_rate: 0.2,
          odds_score: 0.3,
          popularity_score: 0.4,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 2,
          tansho_odds: 3,
          umaban: 1,
        },
      ],
    }),
  ).toStrictEqual({ reason: "duplicate-horse-number", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for a short live odds board", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map([[1, { tanshoNinkijun: 1, tanshoOdds: 2 }]]),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
        {
          career_win_rate: 0.2,
          odds_score: 0.3,
          popularity_score: 0.4,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 2,
          tansho_odds: 3,
          umaban: 2,
        },
      ],
    }),
  ).toStrictEqual({ reason: "partial-live-odds", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for an equal-size wrong live odds board", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map([
        [1, { tanshoNinkijun: 1, tanshoOdds: 2 }],
        [3, { tanshoNinkijun: 2, tanshoOdds: 3 }],
      ]),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
        {
          career_win_rate: 0.2,
          odds_score: 0.3,
          popularity_score: 0.4,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 2,
          tansho_odds: 3,
          umaban: 2,
        },
      ],
    }),
  ).toStrictEqual({ reason: "partial-live-odds", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for invalid live odds", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map([[1, { tanshoNinkijun: 1, tanshoOdds: 0 }]]),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: 0.2,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
      ],
    }),
  ).toStrictEqual({ reason: "invalid-live-odds", status: "fallback" });
});

test("materializeRaceMarketSignals returns fallback for a non-numeric cached input", () => {
  expect(
    materializeRaceMarketSignals({
      liveOddsByHorseNumber: new Map(),
      raceId: "jra:2026:0824:05:11",
      rows: [
        {
          career_win_rate: 0.1,
          odds_score: true,
          popularity_score: 0.3,
          race_id: "jra:2026:0824:05:11",
          tansho_ninkijun: 1,
          tansho_odds: 2,
          umaban: 1,
        },
      ],
    }),
  ).toStrictEqual({ reason: "invalid-cached-input", status: "fallback" });
});

test("materializeRaceMarketSignals matches DuckDB when every cached odds value is non-positive", () => {
  const result = materializeRaceMarketSignals({
    liveOddsByHorseNumber: new Map(),
    raceId: "jra:2026:0824:05:11",
    rows: [
      {
        career_win_rate: 0.1,
        odds_score: 0.2,
        popularity_score: 0.3,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: 1,
        tansho_odds: 0,
        umaban: "1",
      },
      {
        career_win_rate: null,
        odds_score: null,
        popularity_score: null,
        race_id: "jra:2026:0824:05:11",
        tansho_ninkijun: null,
        tansho_odds: -1,
        umaban: 2,
      },
    ],
  });

  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("expected ready market-signal result");
  expect(result.rows[0]?.inverse_odds_implied_prob).toBeNull();
  expect(result.rows[0]?.inverse_odds_market_share).toBeNull();
  expect(result.rows[0]?.inverse_odds_rank_in_race).toBe(1);
  expect(result.rows[1]?.inverse_odds_rank_in_race).toBe(1);
  expect(result.rows[0]?.popularity_rank_in_race).toBe(1);
  expect(result.rows[1]?.popularity_rank_in_race).toBe(2);
});
