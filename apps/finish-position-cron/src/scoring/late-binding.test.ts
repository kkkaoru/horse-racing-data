// Run with: bun run --filter finish-position-cron test
import { expect, test } from "vitest";
import { computeLateBindingColumns } from "./late-binding";

test("computeLateBindingColumns derives odds_score via ln(odds)/ln(300) clamp", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 16, tanshoNinkijun: 3, tanshoOdds: 3.5 },
    weight: { currentBataiju: 480, weightAvg5: 472 },
  });
  expect(result.oddsScore).toBe(Math.log(3.5) / Math.log(300));
  expect(result.tanshoOdds).toBe(3.5);
  expect(result.tanshoNinkijun).toBe(3);
});

test("computeLateBindingColumns derives popularity_score via (ninkijun-1)/(runner-1)", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 16, tanshoNinkijun: 4, tanshoOdds: 5 },
    weight: { currentBataiju: 500, weightAvg5: 490 },
  });
  expect(result.popularityScore).toBe(3 / 15);
});

test("computeLateBindingColumns derives weight_diff_from_avg as current - avg5", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 12, tanshoNinkijun: 2, tanshoOdds: 2.2 },
    weight: { currentBataiju: 488, weightAvg5: 480 },
  });
  expect(result.weightDiffFromAvg).toBe(8);
});

test("computeLateBindingColumns clamps odds_score to 1 for very long odds", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 18, tanshoNinkijun: 18, tanshoOdds: 999 },
    weight: { currentBataiju: 470, weightAvg5: 470 },
  });
  expect(result.oddsScore).toBe(1);
});

test("computeLateBindingColumns floors odds < 1 to ln(1)=0 before clamp", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 10, tanshoNinkijun: 1, tanshoOdds: 0.5 },
    weight: { currentBataiju: 460, weightAvg5: 455 },
  });
  expect(result.oddsScore).toBe(0);
});

test("computeLateBindingColumns falls back to JRA odds median when odds missing", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 14, tanshoNinkijun: null, tanshoOdds: null },
    weight: { currentBataiju: 470, weightAvg5: 468 },
  });
  expect(result.oddsScore).toBe(0.5664);
  expect(result.popularityScore).toBe(0.5);
});

test("computeLateBindingColumns falls back to NAR odds median for nar category", () => {
  const result = computeLateBindingColumns({
    category: "nar",
    odds: { runnerCount: 12, tanshoNinkijun: null, tanshoOdds: 0 },
    weight: { currentBataiju: 450, weightAvg5: 450 },
  });
  expect(result.oddsScore).toBe(0.5048);
  expect(result.popularityScore).toBe(0.5);
});

test("computeLateBindingColumns uses NAR medians for ban-ei category", () => {
  const result = computeLateBindingColumns({
    category: "ban-ei",
    odds: { runnerCount: null, tanshoNinkijun: 3, tanshoOdds: -1 },
    weight: { currentBataiju: 900, weightAvg5: 880 },
  });
  expect(result.oddsScore).toBe(0.5048);
  expect(result.popularityScore).toBe(0.5);
});

test("computeLateBindingColumns yields popularity median when runner_count is 1", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 1, tanshoNinkijun: 1, tanshoOdds: 1.1 },
    weight: { currentBataiju: 470, weightAvg5: 470 },
  });
  expect(result.popularityScore).toBe(0.5);
});

test("computeLateBindingColumns clamps popularity_score above 1 to 1", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 3, tanshoNinkijun: 9, tanshoOdds: 50 },
    weight: { currentBataiju: 470, weightAvg5: 470 },
  });
  expect(result.popularityScore).toBe(1);
});

test("computeLateBindingColumns returns null weight_diff when bataiju missing", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 12, tanshoNinkijun: 2, tanshoOdds: 2.2 },
    weight: { currentBataiju: null, weightAvg5: 480 },
  });
  expect(result.weightDiffFromAvg).toBe(null);
});

test("computeLateBindingColumns returns null weight_diff when weight_avg_5 missing", () => {
  const result = computeLateBindingColumns({
    category: "jra",
    odds: { runnerCount: 12, tanshoNinkijun: 2, tanshoOdds: 2.2 },
    weight: { currentBataiju: 488, weightAvg5: null },
  });
  expect(result.weightDiffFromAvg).toBe(null);
});
