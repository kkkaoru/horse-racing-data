import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const readSql = (name: string): string => readFileSync(resolve(process.cwd(), "sql", name), "utf8");

const migration = readSql("20260815070000_create_oversea_horse_race_history_supplement.sql");
const seed = readSql("20260815070100_seed_jacques_le_marois_jra_official_history_supplement.sql");

it("stores official supplements without blocking parent snapshot refreshes", () => {
  expect(migration).toMatch(/primary key \(history_id, supplement_source\)/u);
  expect(migration).not.toMatch(/references oversea_horse_race_history/u);
  expect(migration).toMatch(/supplement_source = 'jra_official'/u);
  expect(migration).toMatch(/corner_positions_text text/u);
  expect(migration).not.toMatch(/smallint\[\]/u);
  expect(migration).toMatch(/race_time_parse_status in \('parsed', 'unparsed', 'missing'\)/u);
  expect(migration).toMatch(/race_time_parse_status = 'unparsed'.*?race_time_seconds is null/su);
});

it("seeds all forty official recent runs through exact source-native identities", () => {
  expect(seed.match(/^    \(\d+, '\d{4}-\d{2}-\d{2}'/gmu)).toHaveLength(40);
  expect(seed.match(/'parsed'/gu)).toHaveLength(40);
  expect(seed.match(/'\d+:\d{2}\.\d'/gu)).toHaveLength(40);
  expect(seed.match(/'\d+(?:,\d+)+'/gu)).toHaveLength(40);
  expect(seed).toMatch(/m\.source = 'jra-van'/u);
  expect(seed).toMatch(/h\.race_date = o\.race_date::date/u);
  expect(seed).toMatch(/on conflict \(history_id, supplement_source\) do nothing/u);
  expect(seed).not.toMatch(/\b(?:delete|truncate|update)\b/iu);
});
