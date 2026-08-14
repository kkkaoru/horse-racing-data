import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const seed = readFileSync(
  resolve(process.cwd(), "sql/20260815050100_seed_jacques_le_marois_person_win_rate_stats.sql"),
  "utf8",
);

it("seeds the 15 eligible Jacques le Marois JV person populations", () => {
  const rows = Array.from(seed.matchAll(/\('jra', '2026', '0816', 'A8', '04'/gu));
  expect(rows).toHaveLength(15);
  expect(seed).toMatch(/'jockey', 'ギュイヨ', '6'.*?, 46, 42,/u);
  expect(seed).toMatch(/'owner', 'キャロットファーム', '3, 4'.*?, 10453, 1027,/u);
  expect(seed).toMatch(/'trainer', 'グラファ', '8'.*?, 30, 24,/u);
  expect(seed).not.toMatch(/ワッテル/u);
});

it("records reproducible scope and calculation metadata", () => {
  expect(seed).toMatch(/2016-08-16 through 2026-08-15/u);
  expect(seed).toMatch(/'all_venues_all_conditions', 10/u);
  expect(seed).toMatch(/calculated_at = '2026-08-15T09:33:00\+09:00'/u);
  expect(seed).toMatch(/on conflict \(/u);
});
