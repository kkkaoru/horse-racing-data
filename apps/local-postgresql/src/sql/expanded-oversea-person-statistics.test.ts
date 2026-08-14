import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const readSql = (name: string): string => readFileSync(resolve(process.cwd(), "sql", name), "utf8");

const migration = readSql("20260815060000_expand_oversea_person_statistics.sql");
const historySeed = readSql("20260815060100_seed_expanded_jacques_le_marois_person_history.sql");
const statsSeed = readSql("20260815060200_seed_expanded_jacques_le_marois_person_stats.sql");

it("preserves missing historical person fields as null with stable source uniqueness", () => {
  expect(migration).toMatch(/alter column venue drop not null/u);
  expect(migration).toMatch(/alter column horse_name drop not null/u);
  expect(migration).toMatch(/alter column surface drop not null/u);
  expect(migration).toMatch(/alter column distance_metres drop not null/u);
  expect(migration).toMatch(/source_missing_horse_uidx/u);
  expect(historySeed.match(/\('netkeiba', '(?:owner|trainer)'/gu)).toHaveLength(3499);
  expect(historySeed).toMatch(/null, null, 7, '7', '芝', 3200/u);
});

it("records complete row-specific JV and netkeiba populations", () => {
  expect(migration).toMatch(/stats_source in \('jv', 'netkeiba'\)/u);
  expect(migration).toMatch(/'all_published_results'/u);
  expect(statsSeed).toMatch(/'owner', 'ヴェルテメール・エ・フレール'.*?'a0006d'.*?42, true, 42/u);
  expect(statsSeed).toMatch(/'owner', 'Mr Saeed Suhail'.*?'a000ee'.*?25, true, 25/u);
  expect(statsSeed).toMatch(/'trainer', 'オブライ'.*?'05518'.*?2015, true, 2015/u);
  expect(statsSeed).toMatch(/'trainer', 'C．フェ'.*?'a064b'.*?26, true, 26/u);
  expect(statsSeed).not.toMatch(/'trainer', '(?:P．ヴァ|J．スタ)'/u);
});
