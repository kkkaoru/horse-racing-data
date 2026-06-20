// Run with: bun run --filter finish-position-cron test
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "vitest";
import {
  buildFeatCacheKey,
  buildPerRaceFeatCacheKey,
  decodeCacheParquet,
  groupRowsByRace,
  refreshLateBindingColumns,
  toJraRaceEntry,
} from "./feature-cache";

const SAMPLE_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "sample-cache.parquet");
const sampleBytes = new Uint8Array(readFileSync(SAMPLE_PARQUET_PATH));

test("buildFeatCacheKey mirrors the container feat-cache key layout", () => {
  expect(buildFeatCacheKey("jra", "20260619")).toBe("feat-cache/jra/20260619/features.parquet");
});

test("buildPerRaceFeatCacheKey nests keibajoCode + raceBango under the run date", () => {
  expect(buildPerRaceFeatCacheKey("jra", "20260620", "05", "09")).toBe(
    "feat-cache/jra/20260620/05/09/features.parquet",
  );
});

test("decodeCacheParquet reads every cached row from the sample parquet", async () => {
  const rows = await decodeCacheParquet(sampleBytes);
  expect(rows.length).toBe(5);
});

test("decodeCacheParquet preserves race_id and ketto columns from the parquet", async () => {
  const rows = await decodeCacheParquet(sampleBytes);
  expect(String(rows[0]?.race_id)).toBe("jra:2026:0614:05:11");
  expect(String(rows[0]?.ketto_toroku_bango)).toBe("2019100001");
});

test("groupRowsByRace splits the day's rows into per-race groups in order", () => {
  const groups = groupRowsByRace([
    { ketto_toroku_bango: "a", race_id: "jra:2026:0614:05:11" },
    { ketto_toroku_bango: "b", race_id: "jra:2026:0614:05:12" },
    { ketto_toroku_bango: "c", race_id: "jra:2026:0614:05:11" },
  ]);
  expect(groups.length).toBe(2);
  expect(groups[0]?.raceId).toBe("jra:2026:0614:05:11");
  expect(groups[0]?.rows.length).toBe(2);
  expect(groups[1]?.raceId).toBe("jra:2026:0614:05:12");
  expect(groups[1]?.rows.length).toBe(1);
});

test("refreshLateBindingColumns overwrites the 5 late-binding columns from fresh odds", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: 484,
    row: {
      odds_score: 0.5664,
      popularity_score: 0.5,
      shusso_tosu: null,
      tansho_ninkijun: null,
      tansho_odds: null,
      weight_avg_5: 476,
      weight_diff_from_avg: null,
    },
    runnerCount: 16,
    tanshoNinkijun: 3,
    tanshoOdds: 3.5,
  });
  expect(refreshed.odds_score).toBe(Math.log(3.5) / Math.log(300));
  expect(refreshed.popularity_score).toBe(2 / 15);
  expect(refreshed.tansho_odds).toBe(3.5);
  expect(refreshed.tansho_ninkijun).toBe(3);
  expect(refreshed.weight_diff_from_avg).toBe(8);
});

test("refreshLateBindingColumns derives popularity_score from the explicit runnerCount", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: { shusso_tosu: null, weight_avg_5: 470 },
    runnerCount: 13,
    tanshoNinkijun: 3,
    tanshoOdds: 5,
  });
  expect(refreshed.popularity_score).toBe(2 / 12);
});

test("refreshLateBindingColumns falls back to the median when runnerCount is null", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: { shusso_tosu: null, weight_avg_5: 470 },
    runnerCount: null,
    tanshoNinkijun: 3,
    tanshoOdds: 5,
  });
  expect(refreshed.popularity_score).toBe(0.5);
});

test("refreshLateBindingColumns falls back to the median when runnerCount is 1", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: { shusso_tosu: null, weight_avg_5: 470 },
    runnerCount: 1,
    tanshoNinkijun: 1,
    tanshoOdds: 5,
  });
  expect(refreshed.popularity_score).toBe(0.5);
});

test("refreshLateBindingColumns ignores the structurally-null cached shusso_tosu", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: { shusso_tosu: null, weight_avg_5: 470 },
    runnerCount: 16,
    tanshoNinkijun: 8,
    tanshoOdds: 20,
  });
  expect(refreshed.popularity_score).toBe(7 / 15);
});

test("refreshLateBindingColumns keeps early-binding columns untouched", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: 484,
    row: { career_win_rate: 0.25, shusso_tosu: null, weight_avg_5: 470 },
    runnerCount: 12,
    tanshoNinkijun: 1,
    tanshoOdds: 2.1,
  });
  expect(refreshed.career_win_rate).toBe(0.25);
});

test("refreshLateBindingColumns falls back to cached odds when realtime is null", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: {
      shusso_tosu: null,
      tansho_ninkijun: 5,
      tansho_odds: 12,
      weight_avg_5: 460,
    },
    runnerCount: 10,
    tanshoNinkijun: null,
    tanshoOdds: null,
  });
  expect(refreshed.tansho_odds).toBe(12);
  expect(refreshed.tansho_ninkijun).toBe(5);
  expect(refreshed.odds_score).toBe(Math.log(12) / Math.log(300));
  expect(refreshed.weight_diff_from_avg).toBe(null);
});

test("refreshLateBindingColumns parses string-valued cached odds from the parquet", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: {
      shusso_tosu: null,
      tansho_ninkijun: "4",
      tansho_odds: "8.0",
      weight_avg_5: 460,
    },
    runnerCount: 10,
    tanshoNinkijun: null,
    tanshoOdds: null,
  });
  expect(refreshed.tansho_odds).toBe(8);
  expect(refreshed.tansho_ninkijun).toBe(4);
});

test("refreshLateBindingColumns treats an empty-string cached odds cell as missing", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: {
      shusso_tosu: null,
      tansho_ninkijun: "",
      tansho_odds: "   ",
      weight_avg_5: 460,
    },
    runnerCount: 10,
    tanshoNinkijun: null,
    tanshoOdds: null,
  });
  expect(refreshed.odds_score).toBe(0.5664);
  expect(refreshed.popularity_score).toBe(0.5);
});

test("refreshLateBindingColumns treats a non-numeric cached odds cell as missing", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: null,
    row: {
      shusso_tosu: null,
      tansho_ninkijun: "n/a",
      tansho_odds: "n/a",
      weight_avg_5: 460,
    },
    runnerCount: 10,
    tanshoNinkijun: null,
    tanshoOdds: null,
  });
  expect(refreshed.odds_score).toBe(0.5664);
});

test("refreshLateBindingColumns treats a NaN-number cached weight_avg_5 as missing", () => {
  const refreshed = refreshLateBindingColumns({
    category: "jra",
    currentBataiju: 480,
    row: { shusso_tosu: null, tansho_ninkijun: 3, tansho_odds: 4, weight_avg_5: Number.NaN },
    runnerCount: 12,
    tanshoNinkijun: null,
    tanshoOdds: null,
  });
  expect(refreshed.weight_diff_from_avg).toBe(null);
});

test("toJraRaceEntry reads ketto + umaban identity, coercing a bigint umaban", () => {
  const entry = toJraRaceEntry({ ketto_toroku_bango: "2019100001", umaban: 7n });
  expect(entry.kettoTorokuBango).toBe("2019100001");
  expect(entry.umaban).toBe(7);
});

test("toJraRaceEntry defaults a missing umaban to 0", () => {
  const entry = toJraRaceEntry({ ketto_toroku_bango: "2019100002" });
  expect(entry.umaban).toBe(0);
});

test("decoded sample parquet groups + converts into scorer entries end to end", async () => {
  const rows = await decodeCacheParquet(sampleBytes);
  const groups = groupRowsByRace(rows);
  const firstRace = groups[0];
  const entries = firstRace?.rows.map(toJraRaceEntry) ?? [];
  expect(entries.length).toBe(3);
  expect(entries[0]?.umaban).toBe(1);
  expect(entries[1]?.kettoTorokuBango).toBe("2019100002");
});
